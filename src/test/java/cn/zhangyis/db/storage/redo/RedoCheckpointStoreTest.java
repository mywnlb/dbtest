package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * R2 redo control 测试：checkpoint label 必须能跨进程重开读取，并在单个 slot 损坏时回退到另一个有效副本。
 */
class RedoCheckpointStoreTest {

    @TempDir
    Path dir;

    /**
     * 验证 {@code emptyControlReturnsInitialCheckpoint} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void emptyControlReturnsInitialCheckpoint() throws Exception {
        Path control = dir.resolve("redo-control");
        try (RedoCheckpointStore store = RedoCheckpointStore.open(control)) {
            RedoCheckpointLabel label = store.readLatest();

            assertEquals(Lsn.of(0), label.checkpointLsn());
            assertEquals(Lsn.of(0), label.currentLsnAtCheckpoint());
            assertEquals(RedoLogBlockCodec.FORMAT_VERSION, label.redoFormatVersion());
        }
        assertEquals(2L * RedoCheckpointStore.SLOT_STRIDE_BYTES, Files.size(control));
    }

    /**
     * 验证 {@code latestValidCheckpointSurvivesReopen} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void latestValidCheckpointSurvivesReopen() {
        Path control = dir.resolve("redo-control");
        try (RedoCheckpointStore store = RedoCheckpointStore.open(control)) {
            store.write(RedoCheckpointLabel.of(Lsn.of(10), Lsn.of(20), 1_000L));
            store.write(RedoCheckpointLabel.of(Lsn.of(30), Lsn.of(40), 2_000L));
        }

        try (RedoCheckpointStore store = RedoCheckpointStore.open(control)) {
            RedoCheckpointLabel label = store.readLatest();

            assertEquals(Lsn.of(30), label.checkpointLsn());
            assertEquals(Lsn.of(40), label.currentLsnAtCheckpoint());
            assertEquals(2_000L, label.createdAtMillis());
        }
    }

    /**
     * 验证 {@code corruptNewestSlotPageFallsBackToOlderValidSlot} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void corruptNewestSlotPageFallsBackToOlderValidSlot() throws Exception {
        Path control = dir.resolve("redo-control");
        try (RedoCheckpointStore store = RedoCheckpointStore.open(control)) {
            store.write(RedoCheckpointLabel.of(Lsn.of(10), Lsn.of(20), 1_000L));
            store.write(RedoCheckpointLabel.of(Lsn.of(30), Lsn.of(40), 2_000L));
        }

        try (FileChannel channel = FileChannel.open(control, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[RedoCheckpointStore.SLOT_STRIDE_BYTES]),
                    RedoCheckpointStore.SLOT_STRIDE_BYTES);
        }

        try (RedoCheckpointStore store = RedoCheckpointStore.open(control)) {
            RedoCheckpointLabel label = store.readLatest();

            assertEquals(Lsn.of(10), label.checkpointLsn());
            assertEquals(Lsn.of(20), label.currentLsnAtCheckpoint());
        }
    }

    /** torn slot 可能只改坏 version 字节；必须先验 CRC，再决定是否为“不支持格式”。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void checksumFailureTakesPrecedenceOverCorruptedControlVersion() throws Exception {
        Path control = dir.resolve("version-torn-control");
        try (RedoCheckpointStore store = RedoCheckpointStore.open(control)) {
            store.write(RedoCheckpointLabel.of(Lsn.of(10), Lsn.of(20), 1_000L));
            store.write(RedoCheckpointLabel.of(Lsn.of(30), Lsn.of(40), 2_000L));
        }
        try (FileChannel channel = FileChannel.open(control, StandardOpenOption.WRITE)) {
            ByteBuffer corruptedVersion = ByteBuffer.allocate(Integer.BYTES).putInt(99);
            corruptedVersion.flip();
            channel.write(corruptedVersion, RedoCheckpointStore.SLOT_STRIDE_BYTES + Integer.BYTES);
        }

        try (RedoCheckpointStore store = RedoCheckpointStore.open(control)) {
            assertEquals(Lsn.of(10), store.readLatest().checkpointLsn());
        }
    }

    /** 已知 v1 control 不能退化成“两个槽都坏→checkpoint 0”，必须明确拒绝旧格式。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void legacyControlVersionIsRejected() throws Exception {
        Path control = dir.resolve("legacy-control");
        ByteBuffer slot = ByteBuffer.allocate(36);
        slot.putInt(RedoCheckpointStore.MAGIC);
        slot.putInt(1);
        slot.putLong(10L);
        slot.putLong(20L);
        slot.putLong(1_000L);
        CRC32 crc = new CRC32();
        crc.update(slot.array(), 0, 32);
        slot.putInt((int) crc.getValue());
        slot.flip();
        try (FileChannel channel = FileChannel.open(control, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            while (slot.hasRemaining()) {
                channel.write(slot);
            }
        }

        assertThrows(RedoLogFormatException.class, () -> RedoCheckpointStore.open(control));
    }

    /** 只读 control 不创建缺失文件，并拒绝 checkpoint 写入。 */
    @Test
    void readOnlyControlNeverCreatesOrWrites() {
        Path missing = dir.resolve("missing-control");
        assertThrows(RedoLogIoException.class, () -> RedoCheckpointStore.openReadOnly(missing));
        assertFalse(Files.exists(missing));

        Path control = dir.resolve("redo-control");
        try (RedoCheckpointStore writable = RedoCheckpointStore.open(control)) {
            writable.write(RedoCheckpointLabel.of(Lsn.of(10), Lsn.of(20), 1_000L));
        }
        try (RedoCheckpointStore readOnly = RedoCheckpointStore.openReadOnly(control)) {
            assertEquals(Lsn.of(10), readOnly.readLatest().checkpointLsn());
            assertThrows(DatabaseValidationException.class,
                    () -> readOnly.write(RedoCheckpointLabel.of(Lsn.of(30), Lsn.of(40), 2_000L)));
        }
    }
}
