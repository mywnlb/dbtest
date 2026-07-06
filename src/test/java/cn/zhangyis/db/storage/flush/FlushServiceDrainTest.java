package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.DirtyPageCandidate;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.checkpoint.CheckpointCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.policy.AdaptiveFlushPolicy;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F2 tablespace drain 测试：flush service 只能 drain 指定 space，且必须在无法清脏时显式 timeout。
 */
class FlushServiceDrainTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE1 = SpaceId.of(1);
    private static final SpaceId SPACE2 = SpaceId.of(2);
    private static final PageId PAGE1 = PageId.of(SPACE1, PageNo.of(2));
    private static final PageId PAGE2 = PageId.of(SPACE2, PageNo.of(2));

    @TempDir
    Path dir;

    /** dirty drain 不能读取 Buffer Pool 内部结构，必须通过门面等待 dirty view 状态变化。 */
    @Test
    void bufferPoolExposesDirtyStateWaitForFlushDrain() {
        boolean found = Arrays.stream(BufferPool.class.getMethods())
                .anyMatch(method -> method.getName().equals("awaitDirtyStateChange")
                        && method.getParameterCount() == 1
                        && method.getParameterTypes()[0].equals(Duration.class)
                        && method.getReturnType().equals(boolean.class));

        assertTrue(found);
    }

    @Test
    void drainTablespaceFlushesOnlyTargetSpace() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            createSpaces(store);
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range1 = appendRedo(redo, PAGE1);
            LogRange range2 = appendRedo(redo, PAGE2);
            writeDirty(pool, PAGE1, range1.end());
            writeDirty(pool, PAGE2, range2.end());
            redo.markClosed(range1);
            redo.markClosed(range2);

            TablespaceDrainResult result = service(pool, store, redo)
                    .drainTablespace(SPACE1, Duration.ofMillis(200));

            assertFalse(result.timedOut());
            assertEquals(SPACE1, result.spaceId());
            assertEquals(1, result.results().size());
            assertEquals(FlushResultStatus.CLEAN, result.results().get(0).status());
            assertEquals(List.of(PAGE2), dirtyPages(pool));
        }
    }

    @Test
    void drainTablespaceReturnsTimeoutWhenTargetPageCannotBeSnapshotted() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            createSpaces(store);
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range1 = appendRedo(redo, PAGE1);
            writeDirty(pool, PAGE1, range1.end());
            redo.markClosed(range1);

            try (PageGuard ignored = pool.getPage(PAGE1, PageLatchMode.SHARED)) {
                TablespaceDrainResult result = service(pool, store, redo)
                        .drainTablespace(SPACE1, Duration.ZERO);

                assertTrue(result.timedOut());
                assertTrue(result.results().isEmpty());
                assertEquals(List.of(PAGE1), dirtyPages(pool));
            }
        }
    }

    /** drain 无清脏进展时应挂在 BufferPool dirty-state condition 上，而不是固定 1ms 自旋。 */
    @Test
    void drainTablespaceWaitsForDirtyStateChangeWhenFlushMakesNoProgress() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool realPool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo-wait.log"))) {
            createSpaces(store);
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range1 = appendRedo(redo, PAGE1);
            writeDirty(realPool, PAGE1, range1.end());
            redo.markClosed(range1);
            CountingDirtyWaitPool pool = new CountingDirtyWaitPool(realPool);

            try (PageGuard ignored = realPool.getPage(PAGE1, PageLatchMode.SHARED)) {
                TablespaceDrainResult result = service(pool, store, redo)
                        .drainTablespace(SPACE1, Duration.ofMillis(20));

                assertTrue(result.timedOut());
                assertTrue(pool.awaitCount() > 0, "drain must wait through BufferPool dirty-state condition");
            }
        }
    }

    /** fixed dirty page 释放后必须唤醒 drain 重试，否则 truncate drain 会等完整超时。 */
    @Test
    void drainTablespaceWakesWhenFixedDirtyPageIsReleased() throws Exception {
        try (PageStore store = new FileChannelPageStore();
             BufferPool realPool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo-release-wait.log"))) {
            createSpaces(store);
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range1 = appendRedo(redo, PAGE1);
            writeDirty(realPool, PAGE1, range1.end());
            redo.markClosed(range1);
            CountingDirtyWaitPool pool = new CountingDirtyWaitPool(realPool);
            FlushService drainService = service(pool, store, redo);
            PageGuard guard = realPool.getPage(PAGE1, PageLatchMode.SHARED);
            try {
                CompletableFuture<TablespaceDrainResult> draining = CompletableFuture.supplyAsync(
                        () -> drainService.drainTablespace(SPACE1, Duration.ofSeconds(2)));
                assertTrue(pool.awaitEntered(Duration.ofSeconds(1)), "drain did not enter dirty-state wait");

                guard.close();
                guard = null;

                TablespaceDrainResult result = draining.get(1, TimeUnit.SECONDS);
                assertFalse(result.timedOut());
                assertTrue(dirtyPages(realPool).isEmpty());
            } finally {
                if (guard != null) {
                    guard.close();
                }
            }
        }
    }

    /** condition 等待超时返回前若其它 flusher 已清掉目标空间，drain 必须重扫谓词而不是误报 timeout。 */
    @Test
    void drainTablespaceRechecksDirtyViewAfterDirtyWaitTimeoutRace() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool realPool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo-race-wait.log"))) {
            createSpaces(store);
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range1 = appendRedo(redo, PAGE1);
            writeDirty(realPool, PAGE1, range1.end());
            redo.markClosed(range1);
            AtomicReference<PageGuard> pinned = new AtomicReference<>(
                    realPool.getPage(PAGE1, PageLatchMode.SHARED));
            FlushCoordinator externalFlush = coordinator(realPool, store, redo);
            CountingDirtyWaitPool pool = new CountingDirtyWaitPool(realPool, () -> {
                PageGuard guard = pinned.getAndSet(null);
                if (guard != null) {
                    guard.close();
                }
                FlushResult cleaned = externalFlush.singlePageFlush(PAGE1);
                assertEquals(FlushResultStatus.CLEAN, cleaned.status(),
                        "race setup must clean the real dirty page before await reports timeout");
            }, true);
            try {
                TablespaceDrainResult result = service(pool, store, redo)
                        .drainTablespace(SPACE1, Duration.ofMillis(100));

                assertFalse(result.timedOut(),
                        "drain must re-scan dirty predicate after wait timeout before returning timedOut");
                assertTrue(dirtyPages(realPool).isEmpty());
                assertEquals(1, pool.awaitCount());
            } finally {
                PageGuard guard = pinned.getAndSet(null);
                if (guard != null) {
                    guard.close();
                }
            }
        }
    }

    /** 截断 marker 要求全局刷过 marker，而不是只清目标空间，否则其它旧脏页仍会阻止 checkpoint。 */
    @Test
    void flushThroughCleansEveryDirtyPageAtOrBeforeMarkerAndAdvancesCheckpoint() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo-barrier.log"))) {
            createSpaces(store);
            RedoLogManager redo = RedoLogManager.durable(repo);
            LogRange range1 = appendRedo(redo, PAGE1);
            LogRange markerRange = appendRedo(redo, PAGE2);
            writeDirty(pool, PAGE1, range1.end());
            writeDirty(pool, PAGE2, markerRange.end());
            redo.markClosed(range1);
            redo.markClosed(markerRange);

            Lsn checkpoint = service(pool, store, redo).flushThrough(markerRange.end(), Duration.ofSeconds(1));

            assertTrue(checkpoint.value() >= markerRange.end().value());
            assertTrue(dirtyPages(pool).isEmpty());
        }
    }

    private FlushService service(BufferPool pool, PageStore store, RedoLogManager redo) {
        FlushCoordinator coordinator = coordinator(pool, store, redo);
        CheckpointCoordinator checkpoint = new CheckpointCoordinator(pool, redo);
        return new FlushService(pool, coordinator, checkpoint, redo, RedoCapacityPolicy.fixed(10_000),
                AdaptiveFlushPolicy.fixed(1, 8));
    }

    private FlushCoordinator coordinator(BufferPool pool, PageStore store, RedoLogManager redo) {
        return new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
    }

    private void createSpaces(PageStore store) {
        store.create(SPACE1, dir.resolve("s1.ibd"), PS, PageNo.of(4));
        store.create(SPACE2, dir.resolve("s2.ibd"), PS, PageNo.of(4));
    }

    private static LogRange appendRedo(RedoLogManager redo, PageId pageId) {
        LogRange range = redo.append(List.of(new PageBytesRecord(pageId, 256, new byte[]{1, 2, 3})));
        redo.flush();
        return range;
    }

    private static void writeDirty(BufferPool pool, PageId pageId, Lsn lsn) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
            guard.writeBytes(256, new byte[]{1, 2, 3});
        }
    }

    private static List<PageId> dirtyPages(BufferPool pool) {
        return pool.dirtyPageCandidates(Lsn.of(Long.MAX_VALUE), pool.capacity())
                .stream()
                .map(DirtyPageCandidate::pageId)
                .toList();
    }

    /** 只统计 dirty-state wait 调用，其余行为委托真实 BufferPool，避免测试 mock 自己的行为。 */
    private static final class CountingDirtyWaitPool implements BufferPool {
        private final BufferPool delegate;
        private final Runnable firstAwaitAction;
        private final boolean firstAwaitReturnsFalse;
        private final AtomicInteger awaitCount = new AtomicInteger();
        private final CountDownLatch awaitEntered = new CountDownLatch(1);

        private CountingDirtyWaitPool(BufferPool delegate) {
            this(delegate, null, false);
        }

        private CountingDirtyWaitPool(BufferPool delegate, Runnable firstAwaitAction, boolean firstAwaitReturnsFalse) {
            this.delegate = delegate;
            this.firstAwaitAction = firstAwaitAction;
            this.firstAwaitReturnsFalse = firstAwaitReturnsFalse;
        }

        private int awaitCount() {
            return awaitCount.get();
        }

        private boolean awaitEntered(Duration timeout) throws InterruptedException {
            return awaitEntered.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public PageGuard getPage(PageId pageId, PageLatchMode mode) {
            return delegate.getPage(pageId, mode);
        }

        @Override
        public PageGuard newPage(PageId pageId, PageLatchMode mode) {
            return delegate.newPage(pageId, mode);
        }

        @Override
        public void prefetch(PageId pageId) {
            delegate.prefetch(pageId);
        }

        @Override
        public List<DirtyPageCandidate> dirtyPageCandidates(Lsn targetLsn, int maxPages) {
            return delegate.dirtyPageCandidates(targetLsn, maxPages);
        }

        @Override
        public boolean awaitDirtyStateChange(Duration timeout) {
            int count = awaitCount.incrementAndGet();
            awaitEntered.countDown();
            if (count == 1 && firstAwaitAction != null) {
                firstAwaitAction.run();
                if (firstAwaitReturnsFalse) {
                    return false;
                }
            }
            return delegate.awaitDirtyStateChange(timeout);
        }

        @Override
        public Optional<FlushPageSnapshot> snapshotForFlush(PageId pageId) {
            return delegate.snapshotForFlush(pageId);
        }

        @Override
        public boolean completeFlush(FlushPageSnapshot snapshot) {
            return delegate.completeFlush(snapshot);
        }

        @Override
        public void failFlush(PageId pageId) {
            delegate.failFlush(pageId);
        }

        @Override
        public Lsn oldestDirtyLsnOr(Lsn cleanBoundary) {
            return delegate.oldestDirtyLsnOr(cleanBoundary);
        }

        @Override
        public boolean hasDirtyPages() {
            return delegate.hasDirtyPages();
        }

        @Override
        public void invalidateTablespace(SpaceId spaceId, Duration timeout) {
            delegate.invalidateTablespace(spaceId, timeout);
        }

        @Override
        public int residentCountInRange(SpaceId spaceId, long firstPageNo, int pageCount) {
            return delegate.residentCountInRange(spaceId, firstPageNo, pageCount);
        }

        @Override
        public int capacity() {
            return delegate.capacity();
        }

        @Override
        public int residentCount() {
            return delegate.residentCount();
        }

        @Override
        public List<PageId> residentPageIds() {
            return delegate.residentPageIds();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
