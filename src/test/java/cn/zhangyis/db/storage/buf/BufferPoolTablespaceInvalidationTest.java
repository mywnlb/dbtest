package cn.zhangyis.db.storage.buf;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 截断前 Buffer Pool 排空测试：等待 fix、拒绝脏帧、成功后彻底移除该空间驻留页，并覆盖 0.22 stale-frame
 * 版本语义。维护窗口开始后必须阻止新 frame admission；否则 truncate/drop 可能在 drain 期间又引入旧版本 frame。
 */
class BufferPoolTablespaceInvalidationTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(80);

    @TempDir
    Path dir;

    private static PageId page(long pageNo) {
        return PageId.of(SPACE, PageNo.of(pageNo));
    }

    /**
     * 验证 {@code waitsForFixedFrameThenEvictsAllSpaceFrames} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void waitsForFixedFrameThenEvictsAllSpaceFrames() throws Exception {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("wait.ibu"), PS, PageNo.of(2));
            PageGuard guard = pool.getPage(page(0), PageLatchMode.SHARED);
            CompletableFuture<Void> invalidation = CompletableFuture.runAsync(() ->
                    pool.invalidateTablespace(SPACE, Duration.ofSeconds(2)));

            TimeUnit.MILLISECONDS.sleep(100);
            assertFalse(invalidation.isDone());
            guard.close();
            invalidation.get(1, TimeUnit.SECONDS);
            assertEquals(0, pool.residentCount());
        }
    }

    /**
     * 验证 {@code rejectsForegroundAdmissionWhileInvalidationWaitsForFixedFrame} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void rejectsForegroundAdmissionWhileInvalidationWaitsForFixedFrame() throws Exception {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("admission.ibu"), PS, PageNo.of(3));
            PageGuard fixed = pool.getPage(page(0), PageLatchMode.SHARED);
            CompletableFuture<Void> invalidation = CompletableFuture.runAsync(() ->
                    pool.invalidateTablespace(SPACE, Duration.ofSeconds(2)));

            TimeUnit.MILLISECONDS.sleep(100);
            assertFalse(invalidation.isDone(), "invalidation 应等待旧 fixed frame 释放");
            assertThrows(BufferPoolStalePageException.class,
                    () -> pool.getPage(page(1), PageLatchMode.SHARED).close(),
                    "维护窗口打开后不能再注册同一表空间的新普通 page handle");

            fixed.close();
            invalidation.get(1, TimeUnit.SECONDS);
        }
    }

    /**
     * 验证 {@code prefetchSkipsDuringInvalidationWindow} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void prefetchSkipsDuringInvalidationWindow() throws Exception {
        try (CountingPageStore store = openCountingStore("prefetch.ibu", 3);
             BufferPool pool = new LruBufferPool(store, PS, 4)) {
            PageGuard fixed = pool.getPage(page(0), PageLatchMode.SHARED);
            CompletableFuture<Void> invalidation = CompletableFuture.runAsync(() ->
                    pool.invalidateTablespace(SPACE, Duration.ofSeconds(2)));

            TimeUnit.MILLISECONDS.sleep(100);
            assertFalse(invalidation.isDone(), "invalidation 应等待旧 fixed frame 释放");
            pool.prefetch(page(1));
            assertEquals(0, store.reads(page(1)), "维护窗口内 read-ahead 只能丢弃，不能装入旧版本 frame");

            fixed.close();
            invalidation.get(1, TimeUnit.SECONDS);
        }
    }

    /**
     * 验证 {@code loadingOwnerDropsPlaceholderWhenInvalidationBeginsBeforePublish} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void loadingOwnerDropsPlaceholderWhenInvalidationBeginsBeforePublish() throws Exception {
        try (BlockingPageStore store = openBlockingStore("loading.ibu", 3);
             BufferPool pool = new LruBufferPool(store, PS, 4)) {
            AtomicReference<Throwable> ownerError = new AtomicReference<>();
            Thread owner = new Thread(() -> {
                try (PageGuard ignored = pool.getPage(page(1), PageLatchMode.SHARED)) {
                    ignored.readInt(0);
                } catch (Throwable failure) {
                    ownerError.set(failure);
                }
            });
            owner.start();
            assertTrue(store.awaitReadEntered(2), "loader 应先注册 LOADING 并进入物理读");

            CompletableFuture<Void> invalidation = CompletableFuture.runAsync(() ->
                    pool.invalidateTablespace(SPACE, Duration.ofSeconds(2)));
            TimeUnit.MILLISECONDS.sleep(100);
            assertFalse(invalidation.isDone(), "invalidation 应等待 LOADING owner 释放 fix");

            store.releaseRead();
            invalidation.get(2, TimeUnit.SECONDS);
            owner.join(2000);

            assertFalse(owner.isAlive(), "LOADING owner 必须被 stale 检查唤醒并退出");
            assertInstanceOf(BufferPoolStalePageException.class, ownerError.get());
            assertEquals(0, pool.residentCount(), "stale LOADING 占位不得留在 page hash");
        }
    }

    /**
     * 验证 {@code rejectsDirtyFrameInsteadOfSilentlyDroppingIt} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsDirtyFrameInsteadOfSilentlyDroppingIt() {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("dirty.ibu"), PS, PageNo.of(2));
            try (PageGuard guard = pool.newPage(page(0), PageLatchMode.EXCLUSIVE)) {
                guard.writeInt(100, 1);
            }
            assertThrows(DirtyTablespaceInvalidationException.class,
                    () -> pool.invalidateTablespace(SPACE, Duration.ofSeconds(1)));
            assertEquals(1, pool.residentCount());
            try (PageGuard guard = pool.getPage(page(1), PageLatchMode.SHARED)) {
                guard.readInt(0);
            }
            assertEquals(2, pool.residentCount(), "invalidate 失败必须 abort 维护窗口，后续普通访问仍可进入");
        }
    }

    /**
     * 验证 {@code fixedFrameWaitHasTimeout} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void fixedFrameWaitHasTimeout() {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("timeout.ibu"), PS, PageNo.of(2));
            try (PageGuard ignored = pool.getPage(page(0), PageLatchMode.SHARED)) {
                assertThrows(BufferPoolInvalidationTimeoutException.class,
                        () -> pool.invalidateTablespace(SPACE, Duration.ofMillis(30)));
            }
        }
    }

    private CountingPageStore openCountingStore(String fileName, int pages) {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve(fileName), PS, PageNo.of(pages));
        return new CountingPageStore(delegate);
    }

    private BlockingPageStore openBlockingStore(String fileName, int pages) {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve(fileName), PS, PageNo.of(pages));
        return new BlockingPageStore(delegate);
    }

    /** 统计 readPage 次数的 PageStore 装饰器；保留真实文件 IO，只观测是否发生旧版本 admission。 */
    private static class CountingPageStore implements PageStore {
        final PageStore delegate;
        private final java.util.Map<PageId, Integer> reads = new java.util.concurrent.ConcurrentHashMap<>();

        CountingPageStore(PageStore delegate) {
            this.delegate = delegate;
        }

        int reads(PageId pageId) {
            return reads.getOrDefault(pageId, 0);
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
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

    /** 在 readPage 入口阻塞一次，构造 LOADING owner 与 invalidate 维护窗口交叉。 */
    private static final class BlockingPageStore extends CountingPageStore {
        private final CountDownLatch readEntered = new CountDownLatch(1);
        private final CountDownLatch releaseRead = new CountDownLatch(1);

        BlockingPageStore(PageStore delegate) {
            super(delegate);
        }

        boolean awaitReadEntered(long seconds) throws InterruptedException {
            return readEntered.await(seconds, TimeUnit.SECONDS);
        }

        void releaseRead() {
            releaseRead.countDown();
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            readEntered.countDown();
            try {
                releaseRead.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            super.readPage(pageId, dst);
        }
    }
}
