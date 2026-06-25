package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Buffer Pool 并发载入测试（Phase B，设计 §7.1/§7.3）。验证 miss 读盘移出 poolLock：不同页 miss 并发读、同页 miss 只读一次、
 * 载入失败清理占位、命中 LOADING 的等待有界（超时/中断不悬挂）。
 */
class BufferPoolConcurrentLoadTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private HookablePageStore openStore(int pages) {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
        return new HookablePageStore(delegate);
    }

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    private static void awaitQuietly(CountDownLatch latch, long seconds) {
        try {
            latch.await(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Thread reader(BufferPool pool, PageId pageId, AtomicReference<Throwable> error,
                                 AtomicInteger valueOut) {
        return new Thread(() -> {
            try (PageGuard g = pool.getPage(pageId, PageLatchMode.SHARED)) {
                if (valueOut != null) {
                    valueOut.set(g.readInt(0));
                }
            } catch (Throwable t) {
                error.set(t);
            }
        });
    }

    /** <b>核心</b>：两个不同页的 miss 读盘必须能并发进行（IO 已移出 poolLock）。插桩读用 latch 证两次读同时在场。 */
    @Test
    void concurrentMissOnDistinctPagesReadInParallel() throws InterruptedException {
        try (HookablePageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            CountDownLatch bothInside = new CountDownLatch(2);
            AtomicInteger concurrent = new AtomicInteger();
            AtomicInteger maxConcurrent = new AtomicInteger();
            store.beforeRead = p -> {
                int now = concurrent.incrementAndGet();
                maxConcurrent.getAndAccumulate(now, Math::max);
                bothInside.countDown();
                awaitQuietly(bothInside, 5); // 等另一个读也进入；串行化则永远等不到第二个
                concurrent.decrementAndGet();
            };

            AtomicReference<Throwable> e1 = new AtomicReference<>();
            AtomicReference<Throwable> e2 = new AtomicReference<>();
            Thread t1 = reader(pool, page(1), e1, null);
            Thread t2 = reader(pool, page(2), e2, null);
            t1.start();
            t2.start();
            t1.join(5000);
            t2.join(5000);

            assertFalse(t1.isAlive() || t2.isAlive(), "两个 getPage 都应完成（读盘并发，非串行）");
            assertNull(e1.get());
            assertNull(e2.get());
            assertEquals(2, maxConcurrent.get(), "两个不同页读盘曾同时在场 → IO 不在 poolLock 内串行");
        }
    }

    /** 同页并发 miss 只发一次读：后到者命中 LOADING、等待 future、醒来重查 residentMap 拿同帧自 fix。 */
    @Test
    void concurrentMissOnSamePageReadsOnce() throws InterruptedException {
        try (HookablePageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            CountDownLatch ownerReading = new CountDownLatch(1);
            CountDownLatch releaseRead = new CountDownLatch(1);
            store.beforeRead = p -> {
                ownerReading.countDown();
                awaitQuietly(releaseRead, 5);
            };

            AtomicReference<Throwable> eA = new AtomicReference<>();
            AtomicReference<Throwable> eB = new AtomicReference<>();
            AtomicInteger vA = new AtomicInteger();
            AtomicInteger vB = new AtomicInteger();
            Thread a = reader(pool, page(3), eA, vA);
            a.start();
            awaitQuietly(ownerReading, 5);     // A 已进入读盘（LOADING 占位已装）
            Thread b = reader(pool, page(3), eB, vB);
            b.start();
            Thread.sleep(150);                 // 给 B 时间命中 LOADING 并进入等待
            releaseRead.countDown();           // 放行 A 的读盘
            a.join(5000);
            b.join(5000);

            assertFalse(a.isAlive() || b.isAlive());
            assertNull(eA.get());
            assertNull(eB.get());
            assertEquals(1, store.reads(page(3)), "同页只读一次");
            assertEquals(vA.get(), vB.get(), "两者读到同一帧内容");
        }
    }

    /** 载入失败必须清理 LOADING 占位、帧回 free list、不留残留；同线程重试可成功再读一次。 */
    @Test
    void failedLoadClearsLoadingPlaceholderAndRetrySucceeds() {
        try (HookablePageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            store.failOnce.add(page(2));

            assertThrows(DatabaseRuntimeException.class,
                    () -> pool.getPage(page(2), PageLatchMode.SHARED).close());
            assertEquals(0, pool.residentCount(), "载入失败不得留下 LOADING 占位");

            try (PageGuard g = pool.getPage(page(2), PageLatchMode.SHARED)) {
                g.readInt(0); // 重试成功
            }
            assertEquals(1, pool.residentCount());
            assertEquals(2, store.reads(page(2)), "失败读 + 成功重读 = 两次");
        }
    }

    /** IO owner 卡死时，命中 LOADING 的等待者必在 load 超时后抛 BufferPoolLoadTimeoutException，不悬挂。 */
    @Test
    void loadWaiterTimesOutWhenOwnerStalls() throws InterruptedException {
        try (HookablePageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4,
                    new MidpointLruReplacementPolicy(System::currentTimeMillis), Duration.ofMillis(200));
            CountDownLatch ownerReading = new CountDownLatch(1);
            CountDownLatch releaseOwner = new CountDownLatch(1);
            store.beforeRead = p -> {
                ownerReading.countDown();
                awaitQuietly(releaseOwner, 10); // owner 长时间卡住
            };

            AtomicReference<Throwable> eA = new AtomicReference<>();
            Thread a = reader(pool, page(5), eA, null);
            a.start();
            awaitQuietly(ownerReading, 5);

            AtomicReference<Throwable> eB = new AtomicReference<>();
            Thread b = reader(pool, page(5), eB, null);
            b.start();
            b.join(5000);
            boolean bHung = b.isAlive();
            Throwable bError = eB.get();

            releaseOwner.countDown(); // 无论断言成败都放行 owner，避免线程泄漏
            a.join(5000);

            assertFalse(bHung, "等待者不得悬挂");
            assertInstanceOf(BufferPoolLoadTimeoutException.class, bError, "超时应抛 load timeout");
        }
    }

    /** 命中 LOADING 的等待者被中断时，恢复中断位并抛 BufferPoolLoadTimeoutException，不悬挂。 */
    @Test
    void loadWaiterInterruptedDoesNotHang() throws InterruptedException {
        try (HookablePageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4,
                    new MidpointLruReplacementPolicy(System::currentTimeMillis), Duration.ofSeconds(30));
            CountDownLatch ownerReading = new CountDownLatch(1);
            CountDownLatch releaseOwner = new CountDownLatch(1);
            store.beforeRead = p -> {
                ownerReading.countDown();
                awaitQuietly(releaseOwner, 10);
            };

            AtomicReference<Throwable> eA = new AtomicReference<>();
            Thread a = reader(pool, page(6), eA, null);
            a.start();
            awaitQuietly(ownerReading, 5);

            AtomicReference<Throwable> eB = new AtomicReference<>();
            Thread b = reader(pool, page(6), eB, null);
            b.start();
            Thread.sleep(150);  // 给 B 时间进入等待
            b.interrupt();
            b.join(5000);
            boolean bHung = b.isAlive();
            Throwable bError = eB.get();

            releaseOwner.countDown();
            a.join(5000);

            assertFalse(bHung, "被中断的等待者不得悬挂");
            assertInstanceOf(BufferPoolLoadTimeoutException.class, bError, "中断应抛 load timeout（恢复中断位）");
        }
    }

    /**
     * 可插桩 PageStore：统计每页读次数，并在每次读前调用 {@code beforeRead} 钩子（测试用它注入阻塞/信号），
     * 以及对 {@code failOnce} 中的页在首次读时抛领域异常。其余物理 API 透明委托底层。
     */
    private static final class HookablePageStore implements PageStore {
        private final PageStore delegate;
        private final java.util.Map<PageId, Integer> reads = new ConcurrentHashMap<>();
        private final Set<PageId> failOnce = ConcurrentHashMap.newKeySet();
        private volatile Consumer<PageId> beforeRead;

        HookablePageStore(PageStore delegate) {
            this.delegate = delegate;
        }

        int reads(PageId pageId) {
            return reads.getOrDefault(pageId, 0);
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            reads.merge(pageId, 1, Integer::sum);
            Consumer<PageId> hook = beforeRead;
            if (hook != null) {
                hook.accept(pageId);
            }
            if (failOnce.remove(pageId)) {
                throw new DatabaseRuntimeException("induced read failure for " + pageId);
            }
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
