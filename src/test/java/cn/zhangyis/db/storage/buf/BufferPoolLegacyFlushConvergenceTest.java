package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 13.1b-pre legacy flush 收敛验收：`flush/flushAll/no-flusher victim fallback` 仍不承诺 WAL-safe，
 * 但进入 PageStore 物理写盘前必须释放 Buffer Pool metadata lock，并复用 dirtyVersion snapshot 协议。
 */
class BufferPoolLegacyFlushConvergenceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(92);

    @TempDir
    Path dir;

    @Test
    void legacyFlushReleasesMetadataLockBeforePageStoreWrite() throws Exception {
        PageId dirty = page(0);
        PageId clean = page(1);
        try (BlockingWritePageStore store = openBlockingWriteStore("flush-boundary.ibu", 4, dirty);
             LruBufferPool pool = new LruBufferPool(store, PS, 2)) {
            dirtyPage(pool, dirty, 0xCAFE);
            loadClean(pool, clean);

            Worker flusher = Worker.start(() -> pool.flush(dirty));
            assertTrue(store.awaitWritesEntered(1, 2), "flush should reach PageStore.writePage");

            Worker cleanReader = Worker.start(() -> loadClean(pool, clean));
            assertTrue(cleanReader.doneWithin(500),
                    "resident clean hit must proceed while legacy flush write is blocked");

            store.releaseWrites();
            flusher.join();
            cleanReader.join();
        }
    }

    @Test
    void legacyFlushAllReleasesMetadataLockBeforeEachWrite() throws Exception {
        PageId dirty = page(0);
        PageId missed = page(2);
        try (BlockingWritePageStore store = openBlockingWriteStore("flush-all-boundary.ibu", 4, dirty);
             LruBufferPool pool = new LruBufferPool(store, PS, 3)) {
            dirtyPage(pool, dirty, 0xAA55);

            Worker flusher = Worker.start(pool::flushAll);
            assertTrue(store.awaitWritesEntered(1, 2), "flushAll should reach first PageStore.writePage");

            Worker missReader = Worker.start(() -> loadClean(pool, missed));
            assertTrue(missReader.doneWithin(500),
                    "same-instance miss must register LOADING while flushAll write is blocked");

            store.releaseWrites();
            flusher.join();
            missReader.join();
        }
    }

    @Test
    void legacyFlushDoesNotClearPageDirtiedAfterSnapshot() throws Exception {
        PageId page = page(0);
        try (BlockingWritePageStore store = openBlockingWriteStore("flush-redirty.ibu", 4, page);
             LruBufferPool pool = new LruBufferPool(store, PS, 2)) {
            dirtyPage(pool, page, 0x1111);

            Worker flusher = Worker.start(() -> pool.flush(page));
            assertTrue(store.awaitWritesEntered(1, 2), "flush should write snapshot image");

            Worker modifier = Worker.start(() -> dirtyPage(pool, page, 0x2222));
            assertTrue(modifier.doneWithin(500),
                    "page may be dirtied again while legacy flush writes an older snapshot");

            store.releaseWrites();
            flusher.join();
            modifier.join();

            assertTrue(pool.snapshotForFlush(page).isPresent(),
                    "completeFlush must keep frame dirty when dirtyVersion changed after snapshot");
        }
    }

    @Test
    void dirtyVictimNoFlusherFallbackWritesOutsideMetadataLock() throws Exception {
        PageId dirty = page(0);
        PageId clean = page(1);
        PageId incoming = page(2);
        ManualReplacementPolicy policy = new ManualReplacementPolicy();
        try (BlockingWritePageStore store = openBlockingWriteStore("victim-fallback-boundary.ibu", 4, dirty);
             LruBufferPool pool = new LruBufferPool(store, PS, 2, policy, Duration.ofSeconds(5))) {
            dirtyPage(pool, dirty, 0xDEAD);
            loadClean(pool, clean);
            policy.forceVictimOrder(dirty, clean);

            try (PageGuard cleanPinned = pool.getPage(clean, PageLatchMode.SHARED)) {
                cleanPinned.readInt(0);
                Worker evictor = Worker.start(() -> loadClean(pool, incoming));
                assertTrue(store.awaitWritesEntered(1, 2), "dirty victim fallback should reach PageStore.writePage");

                Worker cleanReader = Worker.start(() -> loadClean(pool, clean));
                assertTrue(cleanReader.doneWithin(500),
                        "resident clean hit must proceed while no-flusher victim fallback writes dirty page");

                store.releaseWrites();
                evictor.join();
                cleanReader.join();
            }
        }
    }

    @Test
    void legacyFlushFailureRestoresDirtyState() {
        PageId page = page(0);
        try (FailOnceWritePageStore store = openFailOnceStore("flush-failure.ibu", 4, page);
             LruBufferPool pool = new LruBufferPool(store, PS, 2)) {
            dirtyPage(pool, page, 0x7788);

            assertThrows(DataFilePhysicalException.class, () -> pool.flush(page));
            pool.flush(page);

            try (PageGuard reread = pool.getPage(page, PageLatchMode.SHARED)) {
                assertEquals(0x7788, reread.readInt(100));
            }
        }
    }

    private BlockingWritePageStore openBlockingWriteStore(String fileName, int pages, PageId... blockedPages) {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve(fileName), PS, PageNo.of(pages));
        return new BlockingWritePageStore(delegate, List.of(blockedPages));
    }

    private FailOnceWritePageStore openFailOnceStore(String fileName, int pages, PageId failingPage) {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve(fileName), PS, PageNo.of(pages));
        return new FailOnceWritePageStore(delegate, failingPage);
    }

    private static PageId page(long pageNo) {
        return PageId.of(SPACE, PageNo.of(pageNo));
    }

    private static void loadClean(BufferPool pool, PageId pageId) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.SHARED)) {
            guard.readInt(0);
        }
    }

    private static void dirtyPage(BufferPool pool, PageId pageId, int value) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeInt(100, value);
        }
    }

    /** PageStore 装饰器基类：保留真实文件 IO，只在测试点观测或阻塞 writePage。 */
    private abstract static class DelegatingPageStore implements PageStore {
        final PageStore delegate;

        DelegatingPageStore(PageStore delegate) {
            this.delegate = delegate;
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
        public void readPage(PageId pageId, ByteBuffer dst) {
            delegate.readPage(pageId, dst);
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

    /** 阻塞指定页的 writePage，用来证明 PageStore IO 期间 metadata lock 已释放。 */
    private static final class BlockingWritePageStore extends DelegatingPageStore {
        private final List<PageId> blockedPages;
        private final CountDownLatch releaseWrites = new CountDownLatch(1);
        private final AtomicInteger writesEntered = new AtomicInteger();

        BlockingWritePageStore(PageStore delegate, List<PageId> blockedPages) {
            super(delegate);
            this.blockedPages = blockedPages;
        }

        boolean awaitWritesEntered(int expected, long seconds) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
            while (System.nanoTime() < deadline) {
                if (writesEntered.get() >= expected) {
                    return true;
                }
                TimeUnit.MILLISECONDS.sleep(10);
            }
            return writesEntered.get() >= expected;
        }

        void releaseWrites() {
            releaseWrites.countDown();
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
            if (blockedPages.contains(pageId)) {
                writesEntered.incrementAndGet();
                try {
                    releaseWrites.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            delegate.writePage(pageId, src);
        }
    }

    /** 第一次写指定页时抛物理 IO 异常，验证 FLUSHING 失败后能回到 DIRTY 并允许重试。 */
    private static final class FailOnceWritePageStore extends DelegatingPageStore {
        private final PageId failingPage;
        private final AtomicBoolean failNext = new AtomicBoolean(true);

        FailOnceWritePageStore(PageStore delegate, PageId failingPage) {
            super(delegate);
            this.failingPage = failingPage;
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
            if (failingPage.equals(pageId) && failNext.compareAndSet(true, false)) {
                throw new DataFilePhysicalException("induced write failure: " + pageId);
            }
            delegate.writePage(pageId, src);
        }
    }

    /** 测试专用替换策略：固定 victimOrder，避免 dirty victim fallback 依赖 midpoint LRU 的时间窗口。 */
    private static final class ManualReplacementPolicy implements ReplacementPolicy {
        private final List<BufferFrame> frames = new ArrayList<>();
        private final List<PageId> forcedOrder = new ArrayList<>();

        @Override
        public void onAccess(BufferFrame frame) {
        }

        @Override
        public void onInsert(BufferFrame frame) {
            if (!frames.contains(frame)) {
                frames.add(frame);
            }
        }

        @Override
        public void onRemove(BufferFrame frame) {
            frames.remove(frame);
        }

        @Override
        public Iterable<BufferFrame> victimOrder() {
            if (forcedOrder.isEmpty()) {
                return List.copyOf(frames);
            }
            List<BufferFrame> ordered = new ArrayList<>();
            for (PageId pageId : forcedOrder) {
                for (BufferFrame frame : frames) {
                    if (pageId.equals(frame.pageId)) {
                        ordered.add(frame);
                    }
                }
            }
            for (BufferFrame frame : frames) {
                if (!ordered.contains(frame)) {
                    ordered.add(frame);
                }
            }
            return ordered;
        }

        void forceVictimOrder(PageId... pageIds) {
            forcedOrder.clear();
            forcedOrder.addAll(List.of(pageIds));
        }
    }

    /** 后台任务封装：有界等待并传播线程异常，避免并发测试静默失败。 */
    private static final class Worker {
        private final Thread thread;
        private final CountDownLatch done = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private Worker(Runnable body) {
            thread = new Thread(() -> {
                try {
                    body.run();
                } catch (Throwable throwable) {
                    failure.set(throwable);
                } finally {
                    done.countDown();
                }
            }, "bp-legacy-flush-test");
            thread.setDaemon(true);
            thread.start();
        }

        static Worker start(Runnable body) {
            return new Worker(body);
        }

        boolean doneWithin(long millis) throws InterruptedException {
            return done.await(millis, TimeUnit.MILLISECONDS);
        }

        void join() throws InterruptedException {
            assertTrue(done.await(2, TimeUnit.SECONDS), "worker did not finish");
            Throwable throwable = failure.get();
            if (throwable != null) {
                throw new AssertionError("worker failed", throwable);
            }
        }
    }
}
