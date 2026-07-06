package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 13.1c pageHashLock + frameMutex 的 TDD 验收：
 * 子锁必须能被运行期守卫识别，且物理 IO 入口不得持有 page hash 或 frame metadata 锁。
 */
class BufferPoolPageHashFrameLockSplitTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(93);

    @TempDir
    Path dir;

    @Test
    void latchSetRejectsIoWhilePageHashLocked() {
        BufferPoolInstanceLatchSet latchSet = new BufferPoolInstanceLatchSet();
        latchSet.lockPageHash();
        try {
            assertThrows(BufferPoolLatchViolationException.class,
                    () -> latchSet.assertMetadataUnlocked("page read"));
        } finally {
            latchSet.unlockPageHash();
        }
    }

    @Test
    void latchSetRejectsIoWhileFrameMutexLocked() {
        BufferPoolInstanceLatchSet latchSet = new BufferPoolInstanceLatchSet();
        BufferFrame frame = new BufferFrame(PS);
        latchSet.lockFrame(frame);
        try {
            assertThrows(BufferPoolLatchViolationException.class,
                    () -> latchSet.assertMetadataUnlocked("legacy page write"));
        } finally {
            latchSet.unlockFrame(frame);
        }
    }

    @Test
    void physicalReadDoesNotHoldPageHashOrFrameMutex() {
        PageId page = page(0);
        try (ObservingPageStore store = openObservingStore("read-observe.ibu");
             BufferPool pool = new LruBufferPool(store, PS, 2)) {
            try (PageGuard ignored = pool.getPage(page, PageLatchMode.SHARED)) {
                ignored.readInt(0);
            }
            assertFalse(store.criticalLockDuringRead.get(),
                    "PageStore.readPage must run after releasing pageHashLock/frameMutex");
        }
    }

    @Test
    void flushCoordinatorWriteDoesNotHoldPageHashOrFrameMutex() {
        PageId page = page(1);
        try (ObservingPageStore store = openObservingStore("flush-observe.ibu");
             BufferPool pool = new LruBufferPool(store, PS, 2)) {
            try (PageGuard guard = pool.getPage(page, PageLatchMode.EXCLUSIVE)) {
                guard.writeInt(0, 0x13_1C);
            }

            FlushCoordinator coordinator = new FlushCoordinator(pool, store, new RedoLogManager(), PS,
                    new NoDoublewriteStrategy(), Duration.ofMillis(50));
            coordinator.singlePageFlush(page);

            assertFalse(store.criticalLockDuringWrite.get(),
                    "FlushCoordinator PageStore.writePage must run after releasing pageHashLock/frameMutex");
        }
    }

    private ObservingPageStore openObservingStore(String fileName) {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve(fileName), PS, PageNo.of(8));
        return new ObservingPageStore(delegate);
    }

    private static PageId page(long pageNo) {
        return PageId.of(SPACE, PageNo.of(pageNo));
    }

    /**
     * 观察物理 IO 入口是否仍持有 Buffer Pool 子锁。它只包装真实 PageStore，不改变页文件语义。
     */
    private static final class ObservingPageStore implements PageStore {
        private final PageStore delegate;
        private final AtomicBoolean criticalLockDuringRead = new AtomicBoolean();
        private final AtomicBoolean criticalLockDuringWrite = new AtomicBoolean();

        ObservingPageStore(PageStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            if (BufferPoolInstanceLatchSet.currentThreadHoldsPageHashOrFrameLock()) {
                criticalLockDuringRead.set(true);
            }
            delegate.readPage(pageId, dst);
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
            if (BufferPoolInstanceLatchSet.currentThreadHoldsPageHashOrFrameLock()) {
                criticalLockDuringWrite.set(true);
            }
            delegate.writePage(pageId, src);
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
