package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * R2 redo control 测试：checkpoint label 必须能跨进程重开读取，并在单个 slot 损坏时回退到另一个有效副本。
 */
class RedoCheckpointStoreTest {

    @TempDir
    Path dir;

    @Test
    void emptyControlReturnsInitialCheckpoint() {
        try (RedoCheckpointStore store = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            RedoCheckpointLabel label = store.readLatest();

            assertEquals(Lsn.of(0), label.checkpointLsn());
            assertEquals(Lsn.of(0), label.currentLsnAtCheckpoint());
        }
    }

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

    @Test
    void corruptNewestSlotFallsBackToOlderValidSlot() throws Exception {
        Path control = dir.resolve("redo-control");
        try (RedoCheckpointStore store = RedoCheckpointStore.open(control)) {
            store.write(RedoCheckpointLabel.of(Lsn.of(10), Lsn.of(20), 1_000L));
            store.write(RedoCheckpointLabel.of(Lsn.of(30), Lsn.of(40), 2_000L));
        }

        try (FileChannel channel = FileChannel.open(control, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[]{0, 0, 0, 0}), RedoCheckpointStore.SLOT_BYTES);
        }

        try (RedoCheckpointStore store = RedoCheckpointStore.open(control)) {
            RedoCheckpointLabel label = store.readLatest();

            assertEquals(Lsn.of(10), label.checkpointLsn());
            assertEquals(Lsn.of(20), label.currentLsnAtCheckpoint());
        }
    }
}
