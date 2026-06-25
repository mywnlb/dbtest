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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Buffer Pool 扫描抗污染集成测试（Phase A，设计 §6.1/§6.4）。用 {@link CountingPageStore} 统计每页的盘读次数：
 * 一页若在内存命中则不产生新读，被淘汰后重取才再读一次——以此判定某页是否仍驻留，从而验证 midpoint LRU。
 */
class BufferPoolScanResistanceTest {

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

    /** 只读触碰一页（命中则不产生盘读），用于驱动 LRU 访问统计。 */
    private static void touch(BufferPool pool, PageId pageId) {
        try (PageGuard g = pool.getPage(pageId, PageLatchMode.SHARED)) {
            g.readInt(0);
        }
    }

    /**
     * <b>默认池接线（wiring driver）</b>：默认构造的 Buffer Pool 必须用 midpoint LRU。判别行为——一张刚被再次访问的
     * 冷页（窗内重访）不因"访问即移 MRU"而免于淘汰：plain LRU 下该页会存活，midpoint LRU 下仍按 old 子链尾被淘汰。
     * 用默认构造器（墙钟）；两次访问间隔为微秒级，远小于 1s 提升窗，故必落"窗内不提升"语义，确定可复现。
     */
    @Test
    void defaultPoolEvictsRecentlyReaccessedColdPageUnlikePlainLru() {
        try (CountingPageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 2);
            touch(pool, page(0)); // 读盘 reads[0]=1，进 old
            touch(pool, page(1)); // 读盘 reads[1]=1，old 满 [0,1]
            touch(pool, page(0)); // 命中再访（窗内）→ midpoint 不移；plain LRU 会移到 MRU 保命
            touch(pool, page(2)); // 触发淘汰：midpoint 淘汰 old 头 page0；plain LRU 淘汰 page1

            assertEquals(1, store.reads(page(0)), "page0 仍应只读过 1 次（被淘汰，但尚未重取）");
            touch(pool, page(0)); // 若已被淘汰则重新读盘
            assertEquals(2, store.reads(page(0)),
                    "midpoint LRU：窗内重访的冷页仍被淘汰，重取需再读盘（plain LRU 下会存活，reads 保持 1）");
            pool.close();
        }
    }

    /**
     * <b>核心价值</b>：一次性大扫描不冲掉已建立的 OLTP 热工作集。用可控时钟把 2 张热页提升进 new 子链，再扫过远超
     * 容量的冷页（各访一次、均在提升窗内）；热页必须全程驻留（重取不再读盘），而早期冷页被淘汰（重取要再读盘）。
     */
    @Test
    void largeScanDoesNotEvictHotWorkingSet() {
        try (CountingPageStore store = openStore(64)) {
            AtomicLong clock = new AtomicLong(0);
            MidpointLruReplacementPolicy policy = new MidpointLruReplacementPolicy(clock::get);
            BufferPool pool = new LruBufferPool(store, PS, 6, policy);

            // 建热工作集：page1/page2 读入(进 old) → 跨过提升窗 → 再访 → 升 new。
            touch(pool, page(1));
            touch(pool, page(2));
            clock.set(2_000); // > oldBlocksTime(1000ms)
            touch(pool, page(1));
            touch(pool, page(2));
            int hot1 = store.reads(page(1));
            int hot2 = store.reads(page(2));

            // 大扫描：20 张冷页，各访一次（远超容量 6）。
            for (long p = 10; p < 30; p++) {
                touch(pool, page(p));
            }

            // 热页全程驻留：重取不产生新盘读。
            touch(pool, page(1));
            touch(pool, page(2));
            assertEquals(hot1, store.reads(page(1)), "热页 page1 扛过扫描，未被淘汰重读");
            assertEquals(hot2, store.reads(page(2)), "热页 page2 扛过扫描，未被淘汰重读");

            // 早期冷页已被淘汰：重取需再读盘。
            int coldBefore = store.reads(page(10));
            touch(pool, page(10));
            assertTrue(store.reads(page(10)) > coldBefore, "早期冷扫描页应已被淘汰");
            pool.close();
        }
    }

    /**
     * 统计每页 readPage 次数的 PageStore 装饰器（仅测试用）。其余物理 API 透明委托底层
     * {@link FileChannelPageStore}，只在读路径插桩，用于判定页是否仍驻留缓冲池。
     */
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
