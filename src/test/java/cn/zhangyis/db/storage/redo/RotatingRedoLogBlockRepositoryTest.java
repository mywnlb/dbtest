package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Redo ring 的 LogBlock v1 测试：轮转边界、文件集/header 可信度和跨文件连续性必须 fail-closed。 */
class RotatingRedoLogBlockRepositoryTest {

    private static final PageId PAGE = PageId.of(SpaceId.of(3), PageNo.of(11));

    @TempDir
    Path dir;

    /** 一个小 batch 占一个 block；容量恰好 512B 时每批轮转，并保持全局 blockNo 连续。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void rotatesWholeBatchAndKeepsGlobalBlockNumbers() throws Exception {
        long l = length(8);
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(dir, 3, 512)) {
            repo.append(batch(0, 8));
            repo.append(batch(l, 8));
            repo.append(batch(2 * l, 8));
            repo.force();
            assertEquals(3, repo.readBatches().size());
        }

        for (int fileId = 0; fileId < 3; fileId++) {
            Path path = ringFile(fileId);
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                ByteBuffer blockNo = ByteBuffer.allocate(Long.BYTES);
                channel.read(blockNo, RotatingRedoLogRepository.FILE_HEADER_BYTES + 8L);
                blockNo.flip();
                assertEquals(fileId, blockNo.getLong());
            }
        }
    }

    /** 物理容量必须按 block 对齐；大于单文件容量的 batch 在触碰文件前拒绝。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void validatesAlignedCapacityAndRejectsOversizedBatch() throws Exception {
        assertThrows(DatabaseValidationException.class,
                () -> RotatingRedoLogRepository.open(dir.resolve("bad"), 2, 513));
        Path ring = dir.resolve("ring");
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(ring, 2, 512)) {
            assertThrows(DatabaseValidationException.class, () -> repo.append(batch(0, 1_400)));
            assertEquals(0, repo.readBatches().size());
        }
    }

    /** 重启配置不能把已有文件的 block 区容量缩到实际内容以下，否则后续轮转边界不再可信。 */
    @Test
    void rejectsExistingFileLargerThanConfiguredBlockCapacity() {
        Path ring = dir.resolve("capacity-mismatch");
        long l = length(8);
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(ring, 2, 1_024)) {
            repo.append(batch(0, 8));
            repo.append(batch(l, 8));
            repo.force();
        }

        assertThrows(RedoLogCorruptedException.class,
                () -> RotatingRedoLogRepository.open(ring, 2, 512));
    }

    /** 逻辑最后文件的坏 checksum 是 torn tail；更早文件同类损坏后仍有后续 redo，必须致命。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void toleratesTornOnlyInLogicalLastFile() throws Exception {
        long l = length(8);
        Path tailRing = dir.resolve("tail");
        writeThreeFiles(tailRing, l);
        corrupt(tailRing.resolve("redo-000002.log"), RotatingRedoLogRepository.FILE_HEADER_BYTES + 40L);
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(tailRing, 3, 512)) {
            assertEquals(2, repo.readBatches().size());
        }

        Path middleRing = dir.resolve("middle");
        writeThreeFiles(middleRing, l);
        corrupt(middleRing.resolve("redo-000001.log"), RotatingRedoLogRepository.FILE_HEADER_BYTES + 40L);
        assertThrows(RedoLogCorruptedException.class,
                () -> RotatingRedoLogRepository.open(middleRing, 3, 512));
    }

    /** 已存在 ring 的预期文件不能缺失；只读打开空目录也不能自动创建文件。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void rejectsPartialFileSetAndReadOnlyDoesNotCreate() throws Exception {
        Path ring = dir.resolve("partial");
        try (RotatingRedoLogRepository ignored = RotatingRedoLogRepository.open(ring, 2, 512)) {
            // fresh 初始化完整集合。
        }
        Files.delete(ring.resolve("redo-000001.log"));
        assertThrows(RedoLogCorruptedException.class,
                () -> RotatingRedoLogRepository.open(ring, 2, 512));

        Path missing = dir.resolve("readonly-missing");
        assertThrows(RedoLogIoException.class,
                () -> RotatingRedoLogRepository.openReadOnly(missing, 2, 512));
        assertFalse(Files.exists(missing));
    }

    /** v1 ring header 是已知旧格式，不能按 CRC 损坏或空闲文件自动重写。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void rejectsLegacyRingHeaderVersion() throws Exception {
        Path ring = dir.resolve("legacy");
        Files.createDirectories(ring);
        for (int fileId = 0; fileId < 2; fileId++) {
            writeLegacyHeader(ring.resolve(String.format("redo-%06d.log", fileId)), fileId);
        }
        assertThrows(RedoLogFormatException.class,
                () -> RotatingRedoLogRepository.open(ring, 2, 512));
    }

    /** read-only ring 可扫描，但 append/force/reclaim 都不能改变文件或内存回收边界。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void readOnlyRingRejectsMutations() throws Exception {
        Path ring = dir.resolve("readonly");
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(ring, 2, 512)) {
            repo.append(batch(0, 8));
            repo.force();
        }
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.openReadOnly(ring, 2, 512)) {
            assertEquals(1, repo.readBatches().size());
            assertThrows(DatabaseValidationException.class, () -> repo.append(batch(length(8), 8)));
            assertThrows(DatabaseValidationException.class, repo::force);
            assertThrows(DatabaseValidationException.class,
                    () -> repo.advanceReclaimBoundary(Lsn.of(length(8))));
        }
    }

    private void writeThreeFiles(Path ring, long l) {
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(ring, 3, 512)) {
            repo.append(batch(0, 8));
            repo.append(batch(l, 8));
            repo.append(batch(2 * l, 8));
            repo.force();
        }
    }

    private Path ringFile(int fileId) {
        return dir.resolve(String.format("redo-%06d.log", fileId));
    }

    private static RedoLogBatch batch(long start, int payloadBytes) {
        PageBytesRecord record = new PageBytesRecord(PAGE, 90, new byte[payloadBytes]);
        return new RedoLogBatch(new LogRange(Lsn.of(start), Lsn.of(start + record.byteLength())), List.of(record));
    }

    private static long length(int payloadBytes) {
        return new PageBytesRecord(PAGE, 90, new byte[payloadBytes]).byteLength();
    }

    private static void corrupt(Path path, long offset) throws Exception {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[]{77}), offset);
            channel.force(true);
        }
    }

    private static void writeLegacyHeader(Path path, int fileId) throws Exception {
        ByteBuffer header = ByteBuffer.allocate(RotatingRedoLogRepository.FILE_HEADER_BYTES);
        header.putInt(RotatingRedoLogRepository.HEADER_MAGIC);
        header.putInt(1);
        header.putInt(fileId);
        header.putInt(0);
        header.putLong(0L);
        CRC32 crc = new CRC32();
        crc.update(header.array(), 0, RotatingRedoLogRepository.HEADER_PREFIX_BYTES);
        header.putInt((int) crc.getValue());
        header.flip();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            while (header.hasRemaining()) {
                channel.write(header);
            }
        }
    }
}
