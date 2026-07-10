package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务恢复 checkpoint sidecar 测试：空文件必须与合法初始高水位可区分，双槽中的单份 torn write 不能
 * 抹掉上一份可用基线。
 */
class TransactionRecoveryCheckpointStoreTest {

    @TempDir
    Path dir;

    /** 空 sidecar 返回 empty，让恢复层根据 redo checkpoint 是否为零决定兼容策略。 */
    @Test
    void emptySidecarHasNoAuthoritativeBaseline() {
        try (TransactionRecoveryCheckpointStore store =
                     TransactionRecoveryCheckpointStore.open(dir.resolve("trx-recovery-control"))) {
            assertTrue(store.readLatest().isEmpty());
        }
    }

    /** 最新有效基线必须跨关闭和重开保留 checkpoint 与两个 next-counter。 */
    @Test
    void latestValidBaselineSurvivesReopen() {
        Path path = dir.resolve("trx-recovery-control");
        try (TransactionRecoveryCheckpointStore store = TransactionRecoveryCheckpointStore.open(path)) {
            store.write(checkpoint(10, 7, 4));
            store.write(checkpoint(30, 12, 8));
        }

        try (TransactionRecoveryCheckpointStore store = TransactionRecoveryCheckpointStore.open(path)) {
            assertEquals(checkpoint(30, 12, 8), store.readLatest().orElseThrow());
        }
    }

    /**
     * 最新槽所在的整个 4 KiB 物理页损坏时仍必须回退旧槽。该测试固定双槽不共享同一物理页，避免一次
     * torn sector/page write 同时抹掉两份 CRC 副本。
     */
    @Test
    void corruptNewestSlotPageFallsBackToOlderValidBaseline() throws Exception {
        Path path = dir.resolve("trx-recovery-control");
        try (TransactionRecoveryCheckpointStore store = TransactionRecoveryCheckpointStore.open(path)) {
            store.write(checkpoint(10, 7, 4));
            store.write(checkpoint(30, 12, 8));
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[TransactionRecoveryCheckpointStore.SLOT_STRIDE_BYTES]),
                    TransactionRecoveryCheckpointStore.SLOT_STRIDE_BYTES);
        }

        try (TransactionRecoveryCheckpointStore store = TransactionRecoveryCheckpointStore.open(path)) {
            assertEquals(checkpoint(10, 7, 4), store.readLatest().orElseThrow());
        }
    }

    private static TransactionRecoveryCheckpoint checkpoint(long lsn, long nextId, long nextNo) {
        return new TransactionRecoveryCheckpoint(
                Lsn.of(lsn), TransactionId.of(nextId), TransactionNo.of(nextNo));
    }
}
