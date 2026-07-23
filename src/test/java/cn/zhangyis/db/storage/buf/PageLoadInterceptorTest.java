package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 发布前拦截器的可见性、dirty 发布、失败回收与 detached MTR adoption 并发测试。 */
class PageLoadInterceptorTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE_ID = SpaceId.of(77);
    private static final PageId PAGE_ID = PageId.of(SPACE_ID, PageNo.of(2));
    private static final int VALUE_OFFSET = 128;

    @TempDir
    Path tempDir;

    /** LOADING future 只能在拦截器完成后唤醒，竞争 reader 不得观察到磁盘旧值。 */
    @Test
    void waitsForInterceptorBeforePublishingToConcurrentReaders() throws Exception {
        try (PageStore pageStore = storeWithValue(10);
             LruBufferPool pool = new LruBufferPool(pageStore, PAGE_SIZE, 4)) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch continueMerge = new CountDownLatch(1);
            pool.attachPageLoadInterceptor(publication -> {
                entered.countDown();
                await(continueMerge, "test merge continuation");
                try (PageGuard pending = publication.claimExclusive()) {
                    pending.writeInt(VALUE_OFFSET, 20);
                }
            });

            CompletableFuture<Integer> owner = CompletableFuture.supplyAsync(() -> readValue(pool));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            CompletableFuture<Integer> waiter = CompletableFuture.supplyAsync(() -> readValue(pool));
            assertFalse(waiter.isDone(), "waiter must remain behind the LOADING future");
            continueMerge.countDown();

            assertEquals(20, owner.get(5, TimeUnit.SECONDS));
            assertEquals(20, waiter.get(5, TimeUnit.SECONDS));
            assertTrue(pool.hasDirtyPages(), "pre-publication mutation must enter the flush-list dirty view");
        }
    }

    /** 已绑定只读父 MTR 时，显式 detached 写 MTR 可认领 pending page 并独立提交 redo/pageLSN。 */
    @Test
    void detachedMiniTransactionAdoptsPendingPageWithoutCorruptingParentBinding() {
        try (PageStore pageStore = storeWithValue(30);
             LruBufferPool pool = new LruBufferPool(pageStore, PAGE_SIZE, 4)) {
            MiniTransactionManager manager = new MiniTransactionManager();
            pool.attachPageLoadInterceptor(publication -> manager.executeDetached(
                    manager.budgetFor(RedoBudgetPurpose.CHANGE_BUFFER_MERGE), child -> {
                        PageGuard pending = child.adoptPendingPage(publication);
                        pending.writeInt(VALUE_OFFSET, 40);
                        return null;
                    }));

            MiniTransaction parent = manager.beginReadOnly();
            PageGuard page = parent.getPage(pool, PAGE_ID, PageLatchMode.SHARED);
            assertEquals(40, page.readInt(VALUE_OFFSET));
            manager.commit(parent);
            assertTrue(pool.hasDirtyPages());
        }
    }

    /** 拦截器在认领/写页前失败时必须移除 LOADING 占位，后续 demand read 可重新读盘。 */
    @Test
    void failureBeforeClaimReclaimsLoadingPlaceholderForRetry() {
        try (PageStore pageStore = storeWithValue(50);
             LruBufferPool pool = new LruBufferPool(pageStore, PAGE_SIZE, 4)) {
            AtomicBoolean first = new AtomicBoolean(true);
            pool.attachPageLoadInterceptor(publication -> {
                if (first.getAndSet(false)) {
                    throw new TestInterceptionException("induced before claim");
                }
            });

            assertThrows(TestInterceptionException.class, () -> readValue(pool));
            assertFalse(pool.isResident(PAGE_ID));
            assertEquals(50, readValue(pool));
        }
    }

    /** 写页之后失败不能把 frame 复位并伪装成可重试普通 load，必须提升为 fail-stop 异常。 */
    @Test
    void failureAfterWriteIsFatalAndNeverPublishesPartialPage() {
        try (PageStore pageStore = storeWithValue(60);
             LruBufferPool pool = new LruBufferPool(pageStore, PAGE_SIZE, 4)) {
            pool.attachPageLoadInterceptor(publication -> {
                try (PageGuard pending = publication.claimExclusive()) {
                    pending.writeInt(VALUE_OFFSET, 70);
                }
                throw new TestInterceptionException("induced after write");
            });

            assertThrows(PagePublicationFatalException.class, () -> readValue(pool));
        }
    }

    /** 拦截器认领后返回但忘记关闭 guard 时，Buffer Pool 必须 fail-stop，不能发布一个仍持 X latch 的 frame。 */
    @Test
    void returningWithClaimedGuardOpenIsFatal() {
        try (PageStore pageStore = storeWithValue(80);
             LruBufferPool pool = new LruBufferPool(pageStore, PAGE_SIZE, 4)) {
            AtomicReference<PageGuard> leaked = new AtomicReference<>();
            pool.attachPageLoadInterceptor(publication -> leaked.set(publication.claimExclusive()));

            assertThrows(PagePublicationFatalException.class, () -> readValue(pool));
            PageGuard guard = leaked.get();
            assertTrue(guard != null, "test interceptor must have claimed the pending page");
            guard.close();
        }
    }

    /** fatal future 必须向所有已等待 loader 的线程传播，不能因 LOADING 占位不可回收而忙循环。 */
    @Test
    void fatalLoadFutureFailurePropagatesToWaiters() {
        PageLoadFuture future = new PageLoadFuture();
        PagePublicationFatalException fatal = new PagePublicationFatalException("induced fatal load");
        future.failExceptionally(fatal);

        assertThrows(PagePublicationFatalException.class,
                () -> future.await(TimeUnit.SECONDS.toNanos(1), PAGE_ID));
    }

    private PageStore storeWithValue(int value) {
        PageStore pageStore = new FileChannelPageStore();
        pageStore.create(SPACE_ID, tempDir.resolve("pages-" + value + ".ibd"), PAGE_SIZE, PageNo.of(8));
        byte[] bytes = new byte[PAGE_SIZE.bytes()];
        ByteBuffer.wrap(bytes).putInt(VALUE_OFFSET, value);
        pageStore.writePage(PAGE_ID, ByteBuffer.wrap(bytes));
        return pageStore;
    }

    private static int readValue(LruBufferPool pool) {
        try (PageGuard page = pool.getPage(PAGE_ID, PageLatchMode.SHARED)) {
            return page.readInt(VALUE_OFFSET);
        }
    }

    private static void await(CountDownLatch latch, String operation) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new TestInterceptionException(operation + " timed out");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new TestInterceptionException(operation + " interrupted", interrupted);
        }
    }

    /** 测试专用运行时故障，保留 cause 以验证 Buffer Pool 包装边界。 */
    private static final class TestInterceptionException extends RuntimeException {
        private TestInterceptionException(String message) {
            super(message);
        }

        private TestInterceptionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
