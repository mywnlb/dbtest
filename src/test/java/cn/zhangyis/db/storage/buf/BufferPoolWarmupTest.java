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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.10b Buffer Pool warmup dump/load 测试：dump 驻留页定位 → 新池 load 预取 → 命中不再读盘；缺失/损坏 dump no-op。
 */
class BufferPoolWarmupTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private CountingPageStore openStore() {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
        return new CountingPageStore(delegate);
    }

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    @Test
    void dumpThenLoadWarmsPages() {
        try (CountingPageStore store = openStore()) {
            Path dumpFile = dir.resolve("bp.dump");
            BufferPoolWarmupService warmup = new BufferPoolWarmupService();

            BufferPool pool1 = new LruBufferPool(store, PS, 8);
            for (long p = 1; p <= 3; p++) {
                try (PageGuard g = pool1.getPage(page(p), PageLatchMode.SHARED)) {
                    g.readInt(0);
                }
            }
            int dumped = warmup.dump(pool1, dumpFile);
            assertEquals(3, dumped, "dump 写出 3 个驻留页定位");
            pool1.close();

            BufferPool pool2 = new LruBufferPool(store, PS, 8);
            int loaded = warmup.load(pool2, dumpFile);
            assertEquals(3, loaded, "load 读回 3 个定位并预取");

            int before = store.reads(page(1));
            try (PageGuard g = pool2.getPage(page(1), PageLatchMode.SHARED)) {
                g.readInt(0);
            }
            assertEquals(before, store.reads(page(1)), "warmup 预取后 getPage 命中，不再产生盘读");
            pool2.close();
        }
    }

    @Test
    void loadOnMissingDumpIsNoOp() {
        try (CountingPageStore store = openStore()) {
            BufferPool pool = new LruBufferPool(store, PS, 8);
            int loaded = new BufferPoolWarmupService().load(pool, dir.resolve("absent.dump"));
            assertEquals(0, loaded, "缺失 dump → load no-op");
            assertEquals(0, pool.residentCount());
            pool.close();
        }
    }

    @Test
    void loadOnCorruptDumpIsNoOp() throws Exception {
        try (CountingPageStore store = openStore()) {
            Path dumpFile = dir.resolve("corrupt.dump");
            Files.write(dumpFile, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
            BufferPool pool = new LruBufferPool(store, PS, 8);
            int loaded = new BufferPoolWarmupService().load(pool, dumpFile);
            assertEquals(0, loaded, "损坏 dump → load no-op，不抛");
            pool.close();
        }
    }

    @Test
    void residentPageIdsSnapshotsResidentPages() {
        try (CountingPageStore store = openStore()) {
            BufferPool pool = new LruBufferPool(store, PS, 8);
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.SHARED)) {
                g.readInt(0);
            }
            try (PageGuard g = pool.getPage(page(2), PageLatchMode.SHARED)) {
                g.readInt(0);
            }
            List<PageId> ids = pool.residentPageIds();
            assertEquals(2, ids.size());
            assertTrue(ids.contains(page(1)));
            assertTrue(ids.contains(page(2)));
            assertFalse(ids.contains(page(3)), "未访问页不在快照中");
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
}
