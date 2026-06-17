package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * R2 checkpoint 持久化测试：内存 safe checkpoint 前进后必须写 redo control label，且不能因后续 dirty oldest 回退而覆盖旧 label。
 */
class CheckpointCoordinatorPersistentTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    @Test
    void advanceCheckpointPersistsRedoControlLabel() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo, checkpointStore);
            Lsn checkpoint = coordinator.advanceCheckpoint();
            RedoCheckpointLabel label = checkpointStore.readLatest();

            assertEquals(redo.flushedToDiskLsn(), checkpoint);
            assertEquals(checkpoint, label.checkpointLsn());
            assertEquals(redo.currentLsn(), label.currentLsnAtCheckpoint());
        }
    }

    @Test
    void persistedCheckpointDoesNotMoveBackwardWhenSafeBoundaryDrops() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo, checkpointStore);
            Lsn first = coordinator.advanceCheckpoint();

            writeDirty(pool, 1);
            Lsn second = coordinator.advanceCheckpoint();
            RedoCheckpointLabel label = checkpointStore.readLatest();

            assertEquals(first, second);
            assertEquals(first, label.checkpointLsn());
        }
    }

    private static void writeDirty(BufferPool pool, long lsn) {
        try (PageGuard guard = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn);
            guard.writeBytes(200, new byte[]{2});
        }
    }
}
