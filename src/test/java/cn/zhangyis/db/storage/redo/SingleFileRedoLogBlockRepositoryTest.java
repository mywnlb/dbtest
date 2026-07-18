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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 单文件 LogBlock repository 测试：尾部可恢复，中段/旧格式拒绝，只读扫描不得产生文件副作用。 */
class SingleFileRedoLogBlockRepositoryTest {

    private static final PageId PAGE = PageId.of(SpaceId.of(2), PageNo.of(9));

    @TempDir
    Path dir;

    /** 每批独立封块，重开后仍按逻辑 LSN 顺序返回完整 batch。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void appendsAlignedBlocksAndReopens() throws Exception {
        Path path = dir.resolve("redo.log");
        try (SingleFileRedoLogRepository repo = SingleFileRedoLogRepository.open(path)) {
            repo.append(batch(0, 8));
            repo.append(batch(length(8), 1_400));
            repo.force();
        }
        assertEquals(0, Files.size(path) % RedoLogBlockCodec.BLOCK_BYTES);

        try (SingleFileRedoLogRepository repo = SingleFileRedoLogRepository.open(path)) {
            List<RedoLogBatch> batches = repo.readBatches();
            assertEquals(2, batches.size());
            assertEquals(0, batches.getFirst().range().start().value());
            assertEquals(length(8), batches.getLast().range().start().value());
        }
    }

    /** 最后一批 checksum 损坏时只保留前批；首次续写必须覆盖 torn block，不能把新 redo 追加到不可达尾部之后。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void overwritesTornTailOnFirstAppend() throws Exception {
        Path path = dir.resolve("redo.log");
        long secondStart = length(8);
        try (SingleFileRedoLogRepository repo = SingleFileRedoLogRepository.open(path)) {
            repo.append(batch(0, 8));
            repo.append(batch(secondStart, 8));
            repo.force();
        }
        corrupt(path, RedoLogBlockCodec.BLOCK_BYTES + 40);

        try (SingleFileRedoLogRepository repo = SingleFileRedoLogRepository.open(path)) {
            assertEquals(1, repo.readBatches().size());
            repo.append(batch(secondStart, 16));
            repo.force();
            assertEquals(2, repo.readBatches().size());
        }
        assertEquals(2L * RedoLogBlockCodec.BLOCK_BYTES, Files.size(path));
    }

    /** 非最后 block 的 checksum 损坏意味着后面仍存在 redo，不能按 torn tail 静默截断。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void middleBlockChecksumDamageIsFatal() throws Exception {
        Path path = dir.resolve("redo.log");
        long l = length(8);
        try (SingleFileRedoLogRepository repo = SingleFileRedoLogRepository.open(path)) {
            repo.append(batch(0, 8));
            repo.append(batch(l, 8));
            repo.append(batch(2 * l, 8));
            repo.force();
        }
        corrupt(path, RedoLogBlockCodec.BLOCK_BYTES + 40);

        assertThrows(RedoLogCorruptedException.class, () -> SingleFileRedoLogRepository.open(path));
    }

    /** 旧裸 frame 必须在打开阶段给出明确格式异常，而不是作为最后 torn block 返回空日志。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void legacyRawFrameIsRejectedOnOpen() throws Exception {
        Path path = dir.resolve("redo.log");
        ByteBuffer legacy = RedoBatchFrameCodec.encodeFrame(batch(0, 8));
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            while (legacy.hasRemaining()) {
                channel.write(legacy);
            }
        }
        assertThrows(RedoLogFormatException.class, () -> SingleFileRedoLogRepository.open(path));
    }

    /** read-only 打开不创建缺失文件，也不允许 append；扫描 torn tail 后文件长度保持原样。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void readOnlyOpenNeverCreatesOrRepairs() throws Exception {
        Path missing = dir.resolve("missing.log");
        assertThrows(RedoLogIoException.class, () -> SingleFileRedoLogRepository.openReadOnly(missing));
        assertFalse(Files.exists(missing));

        Path path = dir.resolve("redo.log");
        try (SingleFileRedoLogRepository repo = SingleFileRedoLogRepository.open(path)) {
            repo.append(batch(0, 8));
            repo.force();
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[]{1, 2, 3}), Files.size(path));
        }
        long tornSize = Files.size(path);
        try (SingleFileRedoLogRepository repo = SingleFileRedoLogRepository.openReadOnly(path)) {
            assertEquals(1, repo.readBatches().size());
            assertThrows(DatabaseValidationException.class, () -> repo.append(batch(length(8), 8)));
        }
        assertEquals(tornSize, Files.size(path));
    }

    private static RedoLogBatch batch(long start, int payloadBytes) {
        PageBytesRecord record = new PageBytesRecord(PAGE, 80, new byte[payloadBytes]);
        return new RedoLogBatch(new LogRange(Lsn.of(start), Lsn.of(start + record.byteLength())), List.of(record));
    }

    private static long length(int payloadBytes) {
        return new PageBytesRecord(PAGE, 80, new byte[payloadBytes]).byteLength();
    }

    private static void corrupt(Path path, long offset) throws Exception {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[]{99}), offset);
            channel.force(true);
        }
    }
}
