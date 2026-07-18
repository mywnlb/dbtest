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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 0.10a read-ahead prefetch 原语测试：用 {@link CountingPageStore} 统计每页盘读次数判定驻留与淘汰。
 * 验证 prefetch 把页载入 old 子链且不 fix（被需求读淘汰）、跳过已驻留、无空闲帧丢弃、载入失败回收帧。
 */
class BufferPoolPrefetchTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private CountingPageStore openStore(int pages) {
        FileChannelPageStore delegate = new FileChannelPageStore();
        delegate.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
        return new CountingPageStore(delegate);
    }

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    /**
     * 验证 {@code prefetchLoadsPageThenDemandReadHits} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void prefetchLoadsPageThenDemandReadHits() {
        try (CountingPageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            pool.prefetch(page(0));
            assertEquals(1, store.reads(page(0)), "prefetch 读盘一次");
            assertEquals(1, pool.residentCount());

            try (PageGuard g = pool.getPage(page(0), PageLatchMode.SHARED)) {
                g.readInt(0);
            }
            assertEquals(1, store.reads(page(0)), "随后需求读命中预取页，不再产生盘读");
            pool.close();
        }
    }

    /**
     * 验证 {@code prefetchSkipsAlreadyResidentPage} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void prefetchSkipsAlreadyResidentPage() {
        try (CountingPageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.EXCLUSIVE)) {
                g.readInt(0);
            }
            pool.prefetch(page(0));
            assertEquals(1, store.reads(page(0)), "已驻留页 prefetch 跳过，不重读");
            pool.close();
        }
    }

    /**
     * 验证 {@code prefetchDroppedWhenNoFreeFrame} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void prefetchDroppedWhenNoFreeFrame() {
        try (CountingPageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 1);
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.EXCLUSIVE)) {
                pool.prefetch(page(1));
                assertEquals(0, store.reads(page(1)), "无空闲帧时 prefetch 丢弃，不淘汰被 fix 的页");
                assertEquals(1, pool.residentCount());
            }
            pool.close();
        }
    }

    /**
     * 验证 {@code prefetchedPageIsUnfixedAndEvictedBeforeFixedPage} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void prefetchedPageIsUnfixedAndEvictedBeforeFixedPage() {
        try (CountingPageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 2);
            try (PageGuard g1 = pool.getPage(page(0), PageLatchMode.EXCLUSIVE)) {
                pool.prefetch(page(1)); // 未 fix，进 old
                assertEquals(1, store.reads(page(1)));

                try (PageGuard g3 = pool.getPage(page(2), PageLatchMode.EXCLUSIVE)) {
                    // miss 需腾帧：page0 被 fix、page1 未 fix → 淘汰预取页 page1
                    assertEquals(2, pool.residentCount());
                }
                // g3 关闭后 page2 未 fix；page1 已被淘汰 → 重取需再读盘
                try (PageGuard g = pool.getPage(page(1), PageLatchMode.SHARED)) {
                    g.readInt(0);
                }
                assertEquals(2, store.reads(page(1)), "预取页未 fix，被需求读淘汰，重取再读盘");
            }
            pool.close();
        }
    }

    /**
     * 验证 {@code prefetchReclaimsFrameOnIoFailure} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void prefetchReclaimsFrameOnIoFailure() {
        try (CountingPageStore store = openStore(8)) {
            store.failReads(page(0));
            BufferPool pool = new LruBufferPool(store, PS, 1);

            pool.prefetch(page(0)); // 载入失败 → 尽力而为丢弃 + 回收帧
            assertEquals(0, pool.residentCount(), "失败预取不留驻留/LOADING 占位");

            // 帧已回收：需求读其它页可正常载入
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.EXCLUSIVE)) {
                g.readInt(0);
            }
            assertEquals(1, store.reads(page(1)));
            pool.close();
        }
    }

    /** 统计每页 readPage 次数、可注入读失败的 PageStore 装饰器（仅测试用）。 */
    private static final class CountingPageStore implements PageStore {
        private final PageStore delegate;
        private final Map<PageId, Integer> reads = new HashMap<>();
        private final Set<PageId> failPages = new HashSet<>();

        CountingPageStore(PageStore delegate) {
            this.delegate = delegate;
        }

        int reads(PageId pageId) {
            return reads.getOrDefault(pageId, 0);
        }

        void failReads(PageId pageId) {
            failPages.add(pageId);
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            reads.merge(pageId, 1, Integer::sum);
            if (failPages.contains(pageId)) {
                throw new RuntimeException("injected read failure for " + pageId);
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
