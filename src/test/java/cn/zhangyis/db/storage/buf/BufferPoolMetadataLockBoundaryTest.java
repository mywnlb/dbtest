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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 13.1a metadata lock 边界验收：本片仍是一把 instance metadata lock，但必须显式保证物理 IO、LOADING
 * future 等待、dirty victim flush 前已经释放该锁，为后续 pageHash/freeList/LRU 子锁拆分留下稳定边界。
 */
class BufferPoolMetadataLockBoundaryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(91);

    @TempDir
    Path dir;

    @Test
    void latchSetRejectsIoWhileMetadataLocked() {
        BufferPoolInstanceLatchSet latchSet = new BufferPoolInstanceLatchSet();
        latchSet.lockMetadata();
        try {
            assertThrows(BufferPoolLatchViolationException.class,
                    () -> latchSet.assertMetadataUnlocked("physical read"));
        } finally {
            latchSet.unlockMetadata();
        }
    }

    @Test
    void metadataLockReleasedBeforePhysicalRead() throws Exception {
        PageId p0 = page(0);
        PageId p1 = page(1);
        try (BlockingReadPageStore store = openBlockingStore("read-boundary.ibu", p0, p1);
             BufferPool pool = new LruBufferPool(store, PS, 2)) {
            Worker first = Worker.start(() -> {
                try (PageGuard ignored = pool.getPage(p0, PageLatchMode.SHARED)) {
                    ignored.readInt(0);
                }
            });
            assertTrue(store.awaitReadEntered(1, 2), "first miss should enter PageStore.readPage");

            Worker second = Worker.start(() -> {
                try (PageGuard ignored = pool.getPage(p1, PageLatchMode.SHARED)) {
                    ignored.readInt(0);
                }
            });
            assertTrue(store.awaitReadEntered(2, 2),
                    "second same-instance miss must reach PageStore while first read is blocked");

            store.releaseReads();
            first.join();
            second.join();
        }
    }

    @Test
    void metadataLockReleasedBeforeLoadingWait() throws Exception {
        PageId loading = page(0);
        PageId resident = page(2);
        try (BlockingReadPageStore store = openBlockingStore("wait-boundary.ibu", loading);
             BufferPool pool = new LruBufferPool(store, PS, 3)) {
            try (PageGuard ignored = pool.getPage(resident, PageLatchMode.SHARED)) {
                ignored.readInt(0);
            }

            Worker owner = Worker.start(() -> {
                try (PageGuard ignored = pool.getPage(loading, PageLatchMode.SHARED)) {
                    ignored.readInt(0);
                }
            });
            assertTrue(store.awaitReadEntered(1, 2), "owner should register LOADING and enter physical read");

            Worker waiter = Worker.start(() -> {
                try (PageGuard ignored = pool.getPage(loading, PageLatchMode.SHARED)) {
                    ignored.readInt(0);
                }
            });
            TimeUnit.MILLISECONDS.sleep(100);
            assertFalse(waiter.doneWithin(100), "waiter should be blocked on PageLoadFuture, not finished yet");

            Worker residentReader = Worker.start(() -> {
                try (PageGuard ignored = pool.getPage(resident, PageLatchMode.SHARED)) {
                    ignored.readInt(0);
                }
            });
            assertTrue(residentReader.doneWithin(500),
                    "resident hit must enter metadata section while another thread waits on PageLoadFuture");

            store.releaseReads();
            owner.join();
            waiter.join();
            residentReader.join();
        }
    }

    @Test
    void metadataLockReleasedBeforeDirtyVictimFlush() throws Exception {
        PageId dirty = page(0);
        PageId clean = page(1);
        PageId incoming = page(2);
        ManualReplacementPolicy policy = new ManualReplacementPolicy();
        try (PageStore store = openStore("dirty-victim-boundary.ibu");
             LruBufferPool pool = new LruBufferPool(store, PS, 2, policy, Duration.ofSeconds(5))) {
            BlockingVictimFlusher flusher = new BlockingVictimFlusher(pool);
            pool.attachVictimFlusher(flusher);
            try (PageGuard guard = pool.getPage(dirty, PageLatchMode.EXCLUSIVE)) {
                guard.writeInt(0, 0xCAFE);
            }
            try (PageGuard cleanPinned = pool.getPage(clean, PageLatchMode.SHARED)) {
                cleanPinned.readInt(0);
                policy.forceVictimOrder(dirty, clean);

                Worker evictor = Worker.start(() -> {
                    try (PageGuard ignored = pool.getPage(incoming, PageLatchMode.SHARED)) {
                        ignored.readInt(0);
                    }
                });
                assertTrue(flusher.awaitEntered(2), "dirty victim should reach attached flusher");

                Worker cleanReader = Worker.start(() -> {
                    try (PageGuard ignored = pool.getPage(clean, PageLatchMode.SHARED)) {
                        ignored.readInt(0);
                    }
                });
                assertTrue(cleanReader.doneWithin(500),
                        "clean resident hit must proceed while dirty victim flusher is blocked");

                flusher.release();
                evictor.join();
                cleanReader.join();
            }
            assertEquals(List.of(dirty), flusher.calls);
        }
    }

    private BlockingReadPageStore openBlockingStore(String fileName, PageId... blockedPages) {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve(fileName), PS, PageNo.of(8));
        return new BlockingReadPageStore(delegate, List.of(blockedPages));
    }

    private PageStore openStore(String fileName) {
        FileChannelPageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve(fileName), PS, PageNo.of(8));
        return store;
    }

    private static PageId page(long pageNo) {
        return PageId.of(SPACE, PageNo.of(pageNo));
    }

    /**
     * 可阻塞指定页 readPage 的真实 PageStore 装饰器。测试只观察是否进入 PageStore，不替换底层文件 IO。
     */
    private static final class BlockingReadPageStore implements PageStore {
        private final PageStore delegate;
        private final List<PageId> blockedPages;
        private final CountDownLatch releaseReads = new CountDownLatch(1);
        private final java.util.concurrent.atomic.AtomicInteger readEntered =
                new java.util.concurrent.atomic.AtomicInteger();

        BlockingReadPageStore(PageStore delegate, List<PageId> blockedPages) {
            this.delegate = delegate;
            this.blockedPages = blockedPages;
        }

        boolean awaitReadEntered(int expected, long seconds) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
            while (System.nanoTime() < deadline) {
                if (readEntered.get() >= expected) {
                    return true;
                }
                TimeUnit.MILLISECONDS.sleep(10);
            }
            return readEntered.get() >= expected;
        }

        void releaseReads() {
            releaseReads.countDown();
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            if (blockedPages.contains(pageId)) {
                readEntered.incrementAndGet();
                try {
                    releaseReads.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
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

    /** 阻塞 dirty victim flush，用于观察 flushVictim 调用期间 metadata lock 是否已释放。 */
    private static final class BlockingVictimFlusher implements DirtyVictimFlusher {
        private final LruBufferPool pool;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final List<PageId> calls = new java.util.concurrent.CopyOnWriteArrayList<>();

        BlockingVictimFlusher(LruBufferPool pool) {
            this.pool = pool;
        }

        @Override
        public boolean flushVictim(PageId pageId) {
            calls.add(pageId);
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return pool.snapshotForFlush(pageId).map(pool::completeFlush).orElse(false);
        }

        boolean awaitEntered(long seconds) throws InterruptedException {
            return entered.await(seconds, TimeUnit.SECONDS);
        }

        void release() {
            release.countDown();
        }
    }

    /** 测试专用替换策略：允许固定 victimOrder，避免依赖 midpoint LRU 的时间/访问排序细节。 */
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

    /** 后台任务封装：用有界 join 暴露线程异常，避免并发测试静默失败。 */
    private static final class Worker {
        private final Thread thread;
        private final CountDownLatch done = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private Worker(Runnable body) {
            this.thread = new Thread(() -> {
                try {
                    body.run();
                } catch (Throwable throwable) {
                    failure.set(throwable);
                } finally {
                    done.countDown();
                }
            }, "bp-lock-boundary-test");
            this.thread.setDaemon(true);
            this.thread.start();
        }

        static Worker start(Runnable body) {
            return new Worker(body);
        }

        boolean doneWithin(long millis) throws InterruptedException {
            boolean completed = done.await(millis, TimeUnit.MILLISECONDS);
            if (completed && failure.get() != null) {
                throw new AssertionError("worker failed", failure.get());
            }
            return completed;
        }

        void join() throws InterruptedException {
            assertTrue(done.await(2, TimeUnit.SECONDS), "worker did not finish");
            if (failure.get() != null) {
                throw new AssertionError("worker failed", failure.get());
            }
        }
    }
}
