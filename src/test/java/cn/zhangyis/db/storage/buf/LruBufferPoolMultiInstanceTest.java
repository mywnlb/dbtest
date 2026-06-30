package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.10d 多 instance buffer pool 行为测试（N>1）：路由/往返、容量求和、per-instance 独立淘汰、局部分片耗尽（无 stealing）、
 * dirty 候选跨 instance 合并按 LSN 升序、oldestDirtyLsn 全局 min、invalidate 跨 instance + all-or-nothing、
 * residentCount/ids/range 聚合、read-ahead 经 facade N>1 仍预取、多线程不同 instance 并行 get。
 */
class LruBufferPoolMultiInstanceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private PageStore openStore(int pages) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
        return store;
    }

    private CountingPageStore openCountingStore(int pages) {
        return new CountingPageStore(openStore(pages));
    }

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    /** 用同参数路由器找出前 howMany 个路由到 instanceIndex 的页（确定，便于跨 instance 断言）。 */
    private static List<PageId> pagesForInstance(int instanceCount, int instanceIndex, int howMany) {
        BufferPoolRouter router = new BufferPoolRouter(instanceCount);
        List<PageId> result = new ArrayList<>();
        long no = 0;
        while (result.size() < howMany) {
            PageId p = page(no);
            if (router.route(p) == instanceIndex) {
                result.add(p);
            }
            no++;
        }
        return result;
    }

    private static void writeInt(BufferPool pool, PageId pageId, int value) {
        try (PageGuard g = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            g.writeInt(0, value);
        }
    }

    private static int readInt(BufferPool pool, PageId pageId) {
        try (PageGuard g = pool.getPage(pageId, PageLatchMode.SHARED)) {
            return g.readInt(0);
        }
    }

    private static void writePageLsn(BufferPool pool, PageId pageId, long lsn) {
        try (PageGuard g = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            g.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn);
        }
    }

    @Test
    void routesAndRoundTripsAcrossInstances() {
        try (PageStore store = openStore(256)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 64, 4);
            assertEquals(64, pool.capacity(), "facade capacity = 各分片容量之和");

            PageId a = pagesForInstance(4, 0, 1).get(0);
            PageId b = pagesForInstance(4, 1, 1).get(0);
            PageId c = pagesForInstance(4, 2, 1).get(0);
            writeInt(pool, a, 0xAAAA);
            writeInt(pool, b, 0xBBBB);
            writeInt(pool, c, 0xCCCC);

            assertEquals(0xAAAA, readInt(pool, a));
            assertEquals(0xBBBB, readInt(pool, b));
            assertEquals(0xCCCC, readInt(pool, c));
            assertEquals(3, pool.residentCount());
            pool.close();
        }
    }

    @Test
    void capacitySplitSumsToTotalWithRemainder() {
        try (PageStore store = openStore(64)) {
            // 10 帧分到 3 分片 = 4+3+3，facade capacity 仍为 10。
            LruBufferPool pool = new LruBufferPool(store, PS, 10, 3);
            assertEquals(10, pool.capacity());
            assertEquals(0, pool.residentCount());
            pool.close();
        }
    }

    @Test
    void perInstanceEvictionDoesNotTouchOtherInstanceHotPage() {
        try (CountingPageStore store = openCountingStore(256)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 8, 2); // 每分片 4 帧
            PageId hot = pagesForInstance(2, 0, 1).get(0);          // instance 0
            List<PageId> fillers = pagesForInstance(2, 1, 12);      // instance 1，远超 4 帧

            readInt(pool, hot);
            assertEquals(1, store.reads(hot), "hot 页首次 demand 读盘一次");
            for (PageId f : fillers) {
                readInt(pool, f); // 猛灌 instance 1，触发其内部淘汰
            }
            // 关键：instance 1 的高压淘汰不应碰 instance 0 的 hot 页（无全局 LRU、无跨分片淘汰）。
            readInt(pool, hot);
            assertEquals(1, store.reads(hot), "instance 1 高压淘汰后 hot 页仍驻留 instance 0（未被重读）");
            pool.close();
        }
    }

    @Test
    void localShardExhaustionThrowsEvenWhenOtherShardFree() {
        try (PageStore store = openStore(256)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4, 2); // 每分片 2 帧
            List<PageId> inst0 = pagesForInstance(2, 0, 3);          // 3 页都落 instance 0

            PageGuard g0 = pool.getPage(inst0.get(0), PageLatchMode.EXCLUSIVE);
            PageGuard g1 = pool.getPage(inst0.get(1), PageLatchMode.EXCLUSIVE);
            try {
                // instance 0 两帧都被 fix，第 3 页无受害者 → 抛耗尽，即便 instance 1 两帧全空（无 work stealing）。
                assertThrows(BufferPoolExhaustedException.class,
                        () -> pool.getPage(inst0.get(2), PageLatchMode.EXCLUSIVE));
            } finally {
                g0.close();
                g1.close();
            }
            pool.close();
        }
    }

    @Test
    void dirtyCandidatesMergeAcrossInstancesSortedByLsn() {
        try (PageStore store = openStore(256)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 64, 4);
            PageId p0 = pagesForInstance(4, 0, 1).get(0);
            PageId p1 = pagesForInstance(4, 1, 1).get(0);
            PageId p2 = pagesForInstance(4, 2, 1).get(0);
            writePageLsn(pool, p2, 30);
            writePageLsn(pool, p0, 10);
            writePageLsn(pool, p1, 20);

            List<DirtyPageCandidate> candidates = pool.dirtyPageCandidates(Lsn.of(100), 10);
            assertEquals(List.of(p0, p1, p2),
                    candidates.stream().map(DirtyPageCandidate::pageId).toList(),
                    "跨 instance 脏页按 oldest LSN 全局升序合并");

            // maxPages 裁剪作用于全局合并结果。
            List<DirtyPageCandidate> top2 = pool.dirtyPageCandidates(Lsn.of(100), 2);
            assertEquals(List.of(p0, p1), top2.stream().map(DirtyPageCandidate::pageId).toList());
            pool.close();
        }
    }

    @Test
    void oldestDirtyLsnIsGlobalMinAcrossInstances() {
        try (PageStore store = openStore(256)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 64, 4);
            assertEquals(Lsn.of(99), pool.oldestDirtyLsnOr(Lsn.of(99)), "无脏页返回 clean boundary");

            writePageLsn(pool, pagesForInstance(4, 0, 1).get(0), 50);
            writePageLsn(pool, pagesForInstance(4, 2, 1).get(0), 10);
            assertEquals(Lsn.of(10), pool.oldestDirtyLsnOr(Lsn.of(99)), "oldest dirty = 跨 instance 全局 min");
            assertTrue(pool.hasDirtyPages());
            pool.close();
        }
    }

    @Test
    void invalidateRemovesSpaceAcrossInstancesAndIsAllOrNothing() {
        try (PageStore store = openStore(256)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 64, 4);
            // 在多个 instance 载入 SPACE 的干净页。
            List<PageId> spacePages = new ArrayList<>();
            spacePages.addAll(pagesForInstance(4, 0, 2));
            spacePages.addAll(pagesForInstance(4, 1, 2));
            spacePages.addAll(pagesForInstance(4, 3, 2));
            for (PageId p : spacePages) {
                readInt(pool, p);
            }
            assertEquals(spacePages.size(), pool.residentCount());

            // (a) 全干净：invalidate 跨 instance 移除该空间全部帧。
            pool.invalidateTablespace(SPACE, Duration.ofSeconds(2));
            assertEquals(0, pool.residentCount(), "SPACE 全部帧跨 instance 移除");

            // (b) all-or-nothing：某 instance 有脏帧 → 整体抛、不移除任何帧。
            for (PageId p : spacePages) {
                readInt(pool, p);
            }
            writePageLsn(pool, spacePages.get(0), 5); // 弄脏其一（其所在 instance 阻断截断）
            int before = pool.residentCount();
            assertThrows(DirtyTablespaceInvalidationException.class,
                    () -> pool.invalidateTablespace(SPACE, Duration.ofSeconds(2)));
            assertEquals(before, pool.residentCount(),
                    "两阶段 invalidate：脏帧导致整体放弃，未移除任何 instance 的帧（无部分失效）");
            pool.close();
        }
    }

    @Test
    void aggregatesResidentCountIdsAndRange() {
        try (PageStore store = openStore(256)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 64, 4);
            for (long no : new long[]{2, 4, 5, 8}) {
                readInt(pool, page(no));
            }
            assertEquals(4, pool.residentCount(), "residentCount = 各 instance 求和");
            assertTrue(pool.residentPageIds().containsAll(
                    List.of(page(2), page(4), page(5), page(8))), "residentPageIds 跨 instance 拼接");
            // 区间 [2,6) = 页 2,3,4,5；驻留 2,4,5（散在各 instance）→ 跨 instance 求和计 3。
            assertEquals(3, pool.residentCountInRange(SPACE, 2, 4),
                    "residentCountInRange 跨 instance 求和（2,4,5 可能落不同分片）");
            assertEquals(0, pool.residentCountInRange(SPACE, 10, 4));
            pool.close();
        }
    }

    @Test
    void readAheadPrefetchesThroughFacadeAtMultipleInstances() {
        try (CountingPageStore store = openCountingStore(256)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 128, 4);
            ReadAheadService service = new ReadAheadService(pool, 4, 8); // linear 阈值 4
            service.start();
            pool.attachReadAheadHook(service);
            try {
                for (long p = 0; p <= 3; p++) {
                    readInt(pool, page(p)); // 顺序访问达阈值 → 预取下一 extent（页 64..127）
                }
                assertTrue(service.awaitIdle(Duration.ofSeconds(2)));
                // prefetch 经 facade 路由到各页归属 instance：下一 extent 首/末页被预取读盘。
                assertEquals(1, store.reads(page(64)), "prefetch 经 facade 在 N>1 下仍补取下一 extent 首页");
                assertEquals(1, store.reads(page(127)), "下一 extent 末页被预取");
            } finally {
                service.stop(Duration.ofSeconds(2));
            }
            pool.close();
        }
    }

    @Test
    void concurrentGetsOnDifferentInstancesSucceed() throws InterruptedException {
        try (PageStore store = openStore(512)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 64, 4);
            int threads = 4;
            int perThread = 40;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            for (int t = 0; t < threads; t++) {
                final int base = t * 100; // 各线程独占不相交页区间，跨各 instance
                Thread worker = new Thread(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            PageId p = page(base + i);
                            writeInt(pool, p, base + i);
                            assertEquals(base + i, readInt(pool, p));
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                });
                worker.setDaemon(true);
                worker.start();
            }
            start.countDown();
            assertTrue(done.await(20, TimeUnit.SECONDS), "并发 get 应在限时内完成（无死锁）");
            assertNull(failure.get(), "并发不同 instance get 不应抛异常或内容错乱");
            pool.close();
        }
    }

    /** 统计每页 readPage 次数的 PageStore 装饰器（仅测试用）。 */
    private static final class CountingPageStore implements PageStore {
        private final PageStore delegate;
        private final Map<PageId, Integer> reads = new HashMap<>();

        CountingPageStore(PageStore delegate) {
            this.delegate = delegate;
        }

        synchronized int reads(PageId pageId) {
            return reads.getOrDefault(pageId, 0);
        }

        @Override
        public synchronized void readPage(PageId pageId, ByteBuffer dst) {
            reads.merge(pageId, 1, Integer::sum);
            delegate.readPage(pageId, dst);
        }

        @Override
        public void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
            delegate.create(spaceId, path, pageSize, initialSizeInPages);
        }

        @Override
        public void open(SpaceId spaceId, Path path, PageSize pageSize) {
            delegate.open(spaceId, path, pageSize);
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
            delegate.writePage(pageId, src);
        }

        @Override
        public PageNo extend(SpaceId spaceId) {
            return delegate.extend(spaceId);
        }

        @Override
        public PageNo currentSizeInPages(SpaceId spaceId) {
            return delegate.currentSizeInPages(spaceId);
        }

        @Override
        public Path pathOf(SpaceId spaceId) {
            return delegate.pathOf(spaceId);
        }

        @Override
        public void force(SpaceId spaceId) {
            delegate.force(spaceId);
        }

        @Override
        public void forceAll() {
            delegate.forceAll();
        }

        @Override
        public void truncate(SpaceId spaceId, PageNo targetSizeInPages) {
            delegate.truncate(spaceId, targetSizeInPages);
        }

        @Override
        public void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages) {
            delegate.ensureCapacity(spaceId, minSizeInPages);
        }

        @Override
        public void close(SpaceId spaceId) {
            delegate.close(spaceId);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
