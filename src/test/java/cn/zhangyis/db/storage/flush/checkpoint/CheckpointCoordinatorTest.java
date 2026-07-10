package cn.zhangyis.db.storage.flush.checkpoint;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.RedoReclaimBoundary;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * F1 checkpoint 测试：safe checkpoint LSN 是 dirty oldest、redo current/closed、redo flushed 的安全交集。
 */
class CheckpointCoordinatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    @Test
    void safeCheckpointDoesNotPassOldestDirtyOrRedoDurable() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range = redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            long durable = redo.flushedToDiskLsn().value();
            writeDirty(pool, durable);
            redo.markClosed(range);

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo);

            assertEquals(Lsn.of(durable), coordinator.computeSafeCheckpointLsn());
            assertEquals(Lsn.of(durable), coordinator.advanceCheckpoint());
            assertEquals(Lsn.of(durable), coordinator.lastCheckpointLsn());
        }
    }

    @Test
    void safeCheckpointUsesClosedLsnInsteadOfCurrentLsn() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo-closed.log"))) {
            store.create(SPACE, dir.resolve("s-closed.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            long durable = redo.flushedToDiskLsn().value();
            writeDirty(pool, durable);

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo);

            assertEquals(Lsn.of(0), coordinator.computeSafeCheckpointLsn(),
                    "dirty page exists but its redo range has not been closed");
        }
    }

    @Test
    void safeCheckpointDoesNotPassUnclosedRedoEvenWhenDirtyViewIsEmpty() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo-unclosed-clean.log"))) {
            store.create(SPACE, dir.resolve("s-unclosed-clean.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo);

            assertEquals(Lsn.of(0), coordinator.computeSafeCheckpointLsn(),
                    "an empty dirty view is not enough while a redo range is not closed");
        }
    }

    @Test
    void safeCheckpointUsesRedoFlushedWhenThereAreNoDirtyPages() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range = redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            redo.markClosed(range);

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo);

            assertEquals(redo.flushedToDiskLsn(), coordinator.computeSafeCheckpointLsn());
        }
    }

    @Test
    void advanceCheckpointPushesRedoReclaimBoundaryWhenAdvanced() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo-reclaim.log"));
             RedoCheckpointStore checkpointStore =
                     RedoCheckpointStore.open(dir.resolve("redo-reclaim-control"))) {
            store.create(SPACE, dir.resolve("s-reclaim.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range = redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            long durable = redo.flushedToDiskLsn().value();
            writeDirty(pool, durable);
            redo.markClosed(range);

            AtomicReference<Lsn> reclaimed = new AtomicReference<>();
            CheckpointCoordinator coordinator =
                    new CheckpointCoordinator(pool, redo, checkpointStore, reclaimed::set);

            assertEquals(Lsn.of(durable), coordinator.advanceCheckpoint());
            assertEquals(Lsn.of(durable), reclaimed.get(),
                    "checkpoint 推进后把已持久 checkpoint LSN 推送给 redo 回收边界");
        }
    }

    @Test
    void doesNotTouchReclaimBoundaryWhenCheckpointDoesNotAdvance() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo-noadvance.log"));
             RedoCheckpointStore checkpointStore =
                     RedoCheckpointStore.open(dir.resolve("redo-noadvance-control"))) {
            store.create(SPACE, dir.resolve("s-noadvance.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            // append 但不 markClosed → closedLsn 仍为 0 → 安全 checkpoint 不前进。
            redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();

            AtomicReference<Lsn> reclaimed = new AtomicReference<>();
            CheckpointCoordinator coordinator =
                    new CheckpointCoordinator(pool, redo, checkpointStore, reclaimed::set);

            assertEquals(Lsn.of(0), coordinator.advanceCheckpoint());
            assertNull(reclaimed.get(), "checkpoint 未推进时不触碰回收边界，避免过早放开覆盖");
        }
    }

    /** reclaim 没有持久 redo label 保护时会覆盖恢复仍需要的日志，构造阶段必须拒绝。 */
    @Test
    void reclaimBoundaryRequiresPersistentCheckpointStore() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("unsafe-reclaim.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);

            assertThrows(DatabaseValidationException.class,
                    () -> new CheckpointCoordinator(pool, redo, null, checkpoint -> { }));
        }
    }

    private static void writeDirty(BufferPool pool, long lsn) {
        try (PageGuard guard = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn);
            guard.writeBytes(200, new byte[]{1});
        }
    }
}
