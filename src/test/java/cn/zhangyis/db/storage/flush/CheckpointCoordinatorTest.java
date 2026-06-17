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
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
            redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            long durable = redo.flushedToDiskLsn().value();
            writeDirty(pool, durable);

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo);

            assertEquals(Lsn.of(durable), coordinator.computeSafeCheckpointLsn());
            assertEquals(Lsn.of(durable), coordinator.advanceCheckpoint());
            assertEquals(Lsn.of(durable), coordinator.lastCheckpointLsn());
        }
    }

    @Test
    void safeCheckpointUsesRedoFlushedWhenThereAreNoDirtyPages() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo);

            assertEquals(redo.flushedToDiskLsn(), coordinator.computeSafeCheckpointLsn());
        }
    }

    private static void writeDirty(BufferPool pool, long lsn) {
        try (PageGuard guard = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn);
            guard.writeBytes(200, new byte[]{1});
        }
    }
}
