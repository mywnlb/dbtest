package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.exception.NoFreeSpaceException;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DiskSpaceManager facade 集成测试：建表空间/用量、建段、分配（fragment→extent）、autoextend、NoFreeSpace、释放、drop 回收。
 */
class DiskSpaceManagerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private interface Body {
        void run(DiskSpaceManager dsm, MiniTransactionManager mgr);
    }

    private void withDsm(Body body) {
        PageStore store = new FileChannelPageStore();
        // pool=64：D4a 后每次 allocatePage 多 fix 一个数据页 X latch 且持到 commit；批量单 MTR 分配(33 页)需更大池。
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            body.run(dsm, mgr);
        }
    }

    @Test
    void createTablespaceThenUsageAndSegment() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SpaceUsage u = dsm.usage(m, SPACE);
            assertEquals(PageNo.of(128), u.currentSizeInPages());
            assertEquals(PageNo.of(0), u.freeLimitPageNo());
            assertEquals(1L, u.nextSegmentId());
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            assertEquals(0, ref.inodeSlot());
            assertEquals(SegmentId.of(1), ref.segmentId());
            assertEquals(2L, dsm.usage(m, SPACE).nextSegmentId());
            mgr.commit(m);
        });
    }

    @Test
    void allocateFreeReallocateRecyclesPage() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            assertEquals(PageId.of(SPACE, PageNo.of(64)), p);
            dsm.freePage(m, ref, p);
            assertEquals(PageId.of(SPACE, PageNo.of(64)), dsm.allocatePage(m, ref));
            mgr.commit(m);
        });
    }

    @Test
    void allocateAutoextendsWhenExhausted() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            for (int i = 0; i < 32; i++) {
                assertEquals(PageId.of(SPACE, PageNo.of(64 + i)), dsm.allocatePage(m, ref));
            }
            assertEquals(PageId.of(SPACE, PageNo.of(128)), dsm.allocatePage(m, ref));
            assertEquals(PageNo.of(192), dsm.usage(m, SPACE).currentSizeInPages());
            mgr.commit(m);
        });
    }

    @Test
    void allocateThrowsNoFreeSpaceOnTinyTablespace() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(4));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            assertThrows(NoFreeSpaceException.class, () -> dsm.allocatePage(m, ref));
            mgr.commit(m);
        });
    }

    @Test
    void dropSegmentReclaimsAndAllowsSlotReuse() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(192));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            for (int i = 0; i < 33; i++) {
                dsm.allocatePage(m, ref);
            }
            dsm.dropSegment(m, ref);
            SegmentRef ref2 = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            assertEquals(0, ref2.inodeSlot());
            PageId again = dsm.allocatePage(m, ref2);
            assertEquals(PageId.of(SPACE, PageNo.of(64)), again);
            mgr.commit(m);
        });
    }

    @Test
    void allocatePageEmitsPageInitAndStampsEnvelope() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            mgr.commit(m);

            boolean hasInit = mgr.redoLogManager().bufferedRecords().stream()
                    .anyMatch(r -> r instanceof PageInitRecord pir
                            && pir.pageId().equals(p) && pir.pageType() == PageType.ALLOCATED);
            assertTrue(hasInit, "allocatePage emits PAGE_INIT(ALLOCATED)");

            Lsn endLsn = mgr.redoLogManager().currentLsn();
            try (PageGuard g = pool.getPage(p, PageLatchMode.SHARED)) {
                FilePageHeader h = PageEnvelope.readHeader(g);
                assertEquals(SPACE, h.spaceId());
                assertEquals(p.pageNo().value(), h.pageNo());
                assertEquals(PageType.ALLOCATED, h.pageType());
                assertEquals(endLsn, PageEnvelope.readPageLsn(g));
            }
        }
    }

    @Test
    void reallocateResidentPageReinitializes() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            dsm.freePage(m, ref, p);
            PageId again = dsm.allocatePage(m, ref); // 命中同一驻留页 → 重初始化，不抛
            assertEquals(p, again);
            mgr.commit(m);
            try (PageGuard g = pool.getPage(again, PageLatchMode.SHARED)) {
                assertEquals(PageType.ALLOCATED, PageEnvelope.readHeader(g).pageType());
            }
        }
    }
}
