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
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * R2 checkpoint 持久化测试：内存 safe checkpoint 前进后必须写 redo control label，且不能因后续 dirty oldest 回退而覆盖旧 label。
 */
class CheckpointCoordinatorPersistentTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    /**
     * 验证 {@code advanceCheckpointPersistsRedoControlLabel} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void advanceCheckpointPersistsRedoControlLabel() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range = redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            redo.markClosed(range);

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo, checkpointStore);
            Lsn checkpoint = coordinator.advanceCheckpoint();
            RedoCheckpointLabel label = checkpointStore.readLatest();

            assertEquals(redo.flushedToDiskLsn(), checkpoint);
            assertEquals(checkpoint, label.checkpointLsn());
            assertEquals(redo.currentLsn(), label.currentLsnAtCheckpoint());
        }
    }

    /**
     * 验证 {@code persistedCheckpointDoesNotMoveBackwardWhenSafeBoundaryDrops} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void persistedCheckpointDoesNotMoveBackwardWhenSafeBoundaryDrops() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range = redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            redo.markClosed(range);
            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo, checkpointStore);
            Lsn first = coordinator.advanceCheckpoint();

            writeDirty(pool, 1);
            Lsn second = coordinator.advanceCheckpoint();
            RedoCheckpointLabel label = checkpointStore.readLatest();

            assertEquals(first, second);
            assertEquals(first, label.checkpointLsn());
        }
    }

    /** 事务基线必须先成功，再写 redo label，最后才能公开 reclaim boundary。 */
    @Test
    void metadataParticipantRunsBeforeRedoLabelAndReclaim() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("ordered-redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("ordered-redo-control"))) {
            store.create(SPACE, dir.resolve("ordered.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range = redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            redo.markClosed(range);
            List<String> events = new ArrayList<>();
            AtomicReference<Lsn> metadataLsn = new AtomicReference<>();
            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo, checkpointStore,
                    checkpoint -> {
                        events.add("sidecar");
                        metadataLsn.set(checkpoint);
                    }, checkpoint -> {
                        assertEquals(checkpoint, checkpointStore.readLatest().checkpointLsn(),
                                "redo label must already be durable before reclaim is published");
                        events.add("reclaim");
                    });

            Lsn published = coordinator.advanceCheckpoint();

            assertEquals(published, metadataLsn.get());
            assertEquals(List.of("sidecar", "reclaim"), events);
        }
    }

    /** sidecar force 失败时，redo label、内存 checkpoint 与 reclaim 必须全部停在旧边界。 */
    @Test
    void metadataFailurePreventsRedoLabelAndReclaimAdvance() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("failed-redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("failed-redo-control"))) {
            store.create(SPACE, dir.resolve("failed.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range = redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            redo.markClosed(range);
            AtomicReference<Lsn> reclaimed = new AtomicReference<>();
            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo, checkpointStore,
                    checkpoint -> {
                        throw new cn.zhangyis.db.storage.recovery.TransactionRecoveryException(
                                "synthetic sidecar force failure");
                    }, reclaimed::set);

            assertThrows(cn.zhangyis.db.storage.recovery.TransactionRecoveryException.class,
                    coordinator::advanceCheckpoint);
            assertEquals(Lsn.of(0), checkpointStore.readLatest().checkpointLsn());
            assertEquals(Lsn.of(0), coordinator.lastCheckpointLsn());
            assertEquals(null, reclaimed.get());
        }
    }

    private static void writeDirty(BufferPool pool, long lsn) {
        try (PageGuard guard = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn);
            guard.writeBytes(200, new byte[]{2});
        }
    }
}
