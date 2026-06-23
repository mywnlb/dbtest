package cn.zhangyis.db.storage.flush.cleaner;

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
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.FlushResultStatus;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.flush.checkpoint.CheckpointCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.policy.AdaptiveFlushPolicy;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F2 page cleaner worker 测试：后台 worker 用显式 request 驱动 flush cycle，并支持可控停止。
 */
class PageCleanerWorkerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));
    private static final PageId REDO_ONLY_PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    @Test
    void requestFlushRunsBackgroundCycleAndStopRejectsFurtherRequests() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
            RedoLogManager redo = RedoLogManager.durable(repo);
            Lsn dirtyLsn = appendDirtyThenGrowRedo(redo);
            writeDirty(pool, PAGE, dirtyLsn);
            FlushService service = service(pool, store, redo);

            try (PageCleanerWorker worker = new PageCleanerWorker(service, 4, Duration.ofMillis(5))) {
                worker.start();
                worker.requestFlush(10);

                assertTrue(worker.awaitIdle(Duration.ofSeconds(2)));
                assertEquals(PageCleanerState.IDLE, worker.state());
                assertTrue(worker.lastCycle().orElseThrow().results().stream()
                        .anyMatch(result -> result.status() == FlushResultStatus.CLEAN));
                assertEquals(Lsn.of(999), pool.oldestDirtyLsnOr(Lsn.of(999)));

                assertTrue(worker.stop(Duration.ofSeconds(2)));
                assertEquals(PageCleanerState.STOPPED, worker.state());
                assertThrows(PageCleanerStoppedException.class, () -> worker.requestFlush(1));
            }
        }
    }

    private FlushService service(BufferPool pool, PageStore store, RedoLogManager redo) {
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        CheckpointCoordinator checkpoint = new CheckpointCoordinator(pool, redo);
        return new FlushService(pool, coordinator, checkpoint, redo, RedoCapacityPolicy.fixed(100),
                AdaptiveFlushPolicy.fixed(1, 8));
    }

    private Lsn appendDirtyThenGrowRedo(RedoLogManager redo) {
        Lsn dirty = appendOneRedo(redo, PAGE);
        for (int i = 0; i < 8; i++) {
            appendOneRedo(redo, REDO_ONLY_PAGE);
        }
        redo.flush();
        return dirty;
    }

    private static Lsn appendOneRedo(RedoLogManager redo, PageId pageId) {
        Lsn lsn = redo.append(List.of(new PageBytesRecord(pageId, 256, new byte[]{1, 2, 3}))).end();
        redo.flush();
        return lsn;
    }

    private static void writeDirty(BufferPool pool, PageId pageId, Lsn lsn) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
            guard.writeBytes(256, new byte[]{1, 2, 3});
        }
    }
}
