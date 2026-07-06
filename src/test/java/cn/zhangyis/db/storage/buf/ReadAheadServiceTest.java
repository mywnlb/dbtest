package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.Lsn;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.10a ReadAheadService 集成测试：顺序访问达阈值 → 后台 worker 预取下一 extent；getPage 钩子驱动；停止后忽略访问。
 */
class ReadAheadServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final long NEXT_EXTENT_FIRST = LinearReadAheadTracker.PAGES_PER_EXTENT; // 64

    @TempDir
    Path dir;

    private CountingPageStore openStore() {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(256));
        return new CountingPageStore(delegate);
    }

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    @Test
    void prefetchesNextExtentOnSequentialRecordAccess() {
        try (CountingPageStore store = openStore()) {
            BufferPool pool = new LruBufferPool(store, PS, 128);
            ReadAheadService service = new ReadAheadService(pool, 4, 8);
            service.start();
            try {
                // 直接喂顺序访问（不经 getPage）：仅预取会产生盘读，隔离验证。
                service.recordAccess(page(0));
                service.recordAccess(page(1));
                service.recordAccess(page(2));
                service.recordAccess(page(3)); // 达阈值 → 入队预取 extent 1（页 64..127）

                assertTrue(service.awaitIdle(Duration.ofSeconds(2)), "worker 应在限时内完成预取并空闲");
                assertEquals(1, store.reads(page(NEXT_EXTENT_FIRST)), "下一 extent 起始页 64 被预取读盘");
                assertEquals(1, store.reads(page(NEXT_EXTENT_FIRST + 63)), "下一 extent 末页 127 被预取");
                assertEquals(0, store.reads(page(200)), "extent 外页未被预取");
                assertEquals(0, store.reads(page(0)), "recordAccess 不载入被访问页本身");
            } finally {
                service.stop(Duration.ofSeconds(2));
            }
            pool.close();
        }
    }

    @Test
    void getPageHookDrivesReadAhead() {
        try (CountingPageStore store = openStore()) {
            LruBufferPool pool = new LruBufferPool(store, PS, 128);
            ReadAheadService service = new ReadAheadService(pool, 4, 8);
            service.start();
            pool.attachReadAheadHook(service);
            try {
                for (long p = 0; p <= 3; p++) {
                    try (PageGuard g = pool.getPage(page(p), PageLatchMode.SHARED)) {
                        g.readInt(0);
                    }
                }
                assertTrue(service.awaitIdle(Duration.ofSeconds(2)));
                assertEquals(1, store.reads(page(NEXT_EXTENT_FIRST)), "getPage 顺序访问经钩子触发预取下一 extent");
            } finally {
                service.stop(Duration.ofSeconds(2));
            }
            pool.close();
        }
    }

    @Test
    void ignoresAccessAfterStop() {
        try (CountingPageStore store = openStore()) {
            BufferPool pool = new LruBufferPool(store, PS, 128);
            ReadAheadService service = new ReadAheadService(pool, 4, 8);
            service.start();
            assertTrue(service.stop(Duration.ofSeconds(2)));
            assertEquals(ReadAheadState.STOPPED, service.state());

            // 停止后 recordAccess 静默忽略，不再预取。
            service.recordAccess(page(0));
            service.recordAccess(page(1));
            service.recordAccess(page(2));
            service.recordAccess(page(3));
            assertEquals(0, store.reads(page(NEXT_EXTENT_FIRST)), "停止后不再调度预取");
            pool.close();
        }
    }

    @Test
    void readAheadServiceRandomPrefetchesMissingExtentPages() {
        try (CountingPageStore store = openStore()) {
            LruBufferPool pool = new LruBufferPool(store, PS, 128);
            // linear 阈值设高（64，不会被 4 次访问触发）；random 阈值 4：同一 extent 驻留达 4 页即补取整 extent。
            ReadAheadService service = new ReadAheadService(pool, 64, 4, 8);
            service.start();
            pool.attachReadAheadHook(service);
            try {
                // 经 getPage 访问 extent 0 内 4 页（钩子上报，驻留数累计）；第 4 次达 random 阈值 → 预取整 extent 0。
                for (long p = 0; p <= 3; p++) {
                    try (PageGuard g = pool.getPage(page(p), PageLatchMode.SHARED)) {
                        g.readInt(0);
                    }
                }
                assertTrue(service.awaitIdle(Duration.ofSeconds(2)));
                assertEquals(1, store.reads(page(5)), "extent 0 内未被访问的缺失页被预取读盘一次");
                assertEquals(1, store.reads(page(63)), "extent 0 末页被预取");
                assertEquals(1, store.reads(page(0)), "已驻留页只 demand 读一次，prefetch 跳过（不重复读盘）");
                assertEquals(0, store.reads(page(NEXT_EXTENT_FIRST)), "linear 阈值未达 → 不预取下一 extent（互不干扰）");
            } finally {
                service.stop(Duration.ofSeconds(2));
            }
            pool.close();
        }
    }

    @Test
    void randomDisabledWhenThresholdZero() {
        try (CountingPageStore store = openStore()) {
            LruBufferPool pool = new LruBufferPool(store, PS, 128);
            // randomThreshold=0 → random 禁用（对齐 MySQL 默认 OFF）；linear 阈值高 → 都不触发。
            ReadAheadService service = new ReadAheadService(pool, 64, 0, 8);
            service.start();
            pool.attachReadAheadHook(service);
            try {
                for (long p = 0; p <= 3; p++) {
                    try (PageGuard g = pool.getPage(page(p), PageLatchMode.SHARED)) {
                        g.readInt(0);
                    }
                }
                assertTrue(service.awaitIdle(Duration.ofSeconds(2)));
                assertEquals(0, store.reads(page(5)), "random 禁用：extent 内缺失页不被预取");
            } finally {
                service.stop(Duration.ofSeconds(2));
            }
            pool.close();
        }
    }

    @Test
    void randomPathDoesNotThrowWhenResidentCountFails() {
        try (CountingPageStore store = openStore()) {
            LruBufferPool real = new LruBufferPool(store, PS, 128);
            BufferPool failing = new ThrowingResidentCountPool(real);
            // linear 阈值 4（会触发预取 extent 1）；random 阈值 4，但 residentCountInRange 抛异常 → 必须被吞。
            ReadAheadService service = new ReadAheadService(failing, 4, 4, 8);
            service.start();
            try {
                // 直接喂顺序访问：random 路径每次抛异常但被吞；linear 路径正常达阈值入队，不被 random 失败影响。
                service.recordAccess(page(0));
                service.recordAccess(page(1));
                service.recordAccess(page(2));
                service.recordAccess(page(3)); // linear 达阈值 → 预取 extent 1
                assertTrue(service.awaitIdle(Duration.ofSeconds(2)), "random 抛异常被吞，service 仍正常推进");
                assertEquals(1, store.reads(page(NEXT_EXTENT_FIRST)),
                        "random 检测失败只丢弃本次 random 预取，linear demand-driven 预取不受影响");
            } finally {
                service.stop(Duration.ofSeconds(2));
            }
            real.close();
        }
    }

    /** residentCountInRange 必抛、其余方法委托真实池的装饰器：验证 random 检测异常被吞、不破坏 demand read。 */
    private static final class ThrowingResidentCountPool implements BufferPool {
        private final BufferPool delegate;

        ThrowingResidentCountPool(BufferPool delegate) {
            this.delegate = delegate;
        }

        @Override
        public int residentCountInRange(SpaceId spaceId, long firstPageNo, int pageCount) {
            throw new DatabaseRuntimeException("resident count failure (test)");
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
