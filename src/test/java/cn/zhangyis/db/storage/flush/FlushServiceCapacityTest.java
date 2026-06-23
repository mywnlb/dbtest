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
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.checkpoint.CheckpointCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.policy.AdaptiveFlushPolicy;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoCapacityPressure;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F2 FlushService capacity cycle 测试：把 R2 redo capacity pressure 接入 F1 flush/checkpoint 原语。
 */
class FlushServiceCapacityTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));
    private static final PageId REDO_ONLY_PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    @Test
    void capacityPressureFlushesDirtyPagesAndAdvancesCheckpoint() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
            RedoLogManager redo = RedoLogManager.durable(repo);
            Lsn dirtyLsn = appendDirtyThenGrowRedo(redo);
            writeDirty(pool, PAGE, dirtyLsn);

            FlushService service = flushService(pool, store, redo, RedoCapacityPolicy.fixed(100));
            FlushCycleResult result = service.flushForCapacity(10);

            assertEquals(RedoCapacityPressure.HARD_LIMIT, result.capacityDecision().pressure());
            assertEquals(1, result.results().size());
            assertEquals(FlushResultStatus.CLEAN, result.results().get(0).status());
            assertTrue(result.checkpointAfter().value() > result.checkpointBefore().value());
            assertEquals(Lsn.of(999), pool.oldestDirtyLsnOr(Lsn.of(999)));
        }
    }

    @Test
    void noCapacityPressureDoesNotFlushDirtyPages() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
            RedoLogManager redo = RedoLogManager.durable(repo);
            Lsn dirtyLsn = appendOneRedo(redo, PAGE);
            writeDirty(pool, PAGE, dirtyLsn);

            FlushService service = flushService(pool, store, redo, RedoCapacityPolicy.fixed(10_000));
            FlushCycleResult result = service.flushForCapacity(10);

            assertEquals(RedoCapacityPressure.NONE, result.capacityDecision().pressure());
            assertTrue(result.results().isEmpty());
            assertEquals(dirtyLsn, pool.oldestDirtyLsnOr(Lsn.of(999)));
            assertEquals(result.checkpointBefore(), result.checkpointAfter());
        }
    }

    private FlushService flushService(BufferPool pool, PageStore store, RedoLogManager redo,
                                      RedoCapacityPolicy capacityPolicy) {
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        CheckpointCoordinator checkpoint = new CheckpointCoordinator(pool, redo);
        return new FlushService(pool, coordinator, checkpoint, redo, capacityPolicy,
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

    private Lsn appendOneRedo(RedoLogManager redo, PageId pageId) {
        Lsn end = redo.append(List.of(new PageBytesRecord(pageId, 256, new byte[]{1, 2, 3}))).end();
        redo.flush();
        return end;
    }

    private static void writeDirty(BufferPool pool, PageId pageId, Lsn lsn) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
            guard.writeBytes(256, new byte[]{1, 2, 3});
        }
    }
}
