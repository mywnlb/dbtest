package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SegmentSpaceService 集成测试：fragment 页分配 + inode 记录、assign extent、segment extent 页分配 + SEG 链迁移、
 * 释放两路回收、释放系统/未分配页拒绝。
 *
 * <p>测试纪律：allocateSlot 只碰 page2；跨页 op（assign/alloc/free）先 page0 后 page2。为避免 page2 先于 page0 的逆序，
 * 把 allocateSlot 放在独立 setup MTR 提交后，再在工作 MTR 内执行跨页 op。
 */
class SegmentSpaceServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);
    private static final int PE = 64;

    @TempDir
    Path dir;

    private SpaceHeaderSnapshot fresh(long sizePages) {
        return new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(sizePages), PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, 80046, 1L);
    }

    interface Ctx {
        void run(SpaceHeaderRepository header, ExtentDescriptorRepository xdes, SegmentInodeRepository inode,
                 Flst flst, FreeExtentService free, SegmentSpaceService seg, MiniTransactionManager mgr);
    }

    private void withCtx(long sizePages, Ctx body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(sizePages));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 16)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, PS);
            SegmentInodeRepository inode = new SegmentInodeRepository(pool, PS);
            Flst flst = new Flst(pool);
            FreeExtentService free = new FreeExtentService(pool, PS, header, xdes, flst);
            SegmentSpaceService seg = new SegmentSpaceService(pool, PS, header, inode, xdes, flst, free);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            header.initialize(init, fresh(sizePages));
            mgr.commit(init);
            body.run(header, xdes, inode, flst, free, seg, mgr);
        }
    }

    @Test
    void allocateFragmentPageRecordsInInodeAndCounts() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            Optional<PageNo> p = seg.allocateFragmentPage(m, SPACE, slot);
            assertEquals(Optional.of(PageNo.of(64)), p);
            assertEquals(Optional.of(PageNo.of(64)), inode.getFragmentPage(m, SPACE, slot, 0));
            assertEquals(1L, inode.read(m, SPACE, slot).usedPageCount());
            mgr.commit(m);
        });
    }

    @Test
    void assignExtentSetsFsegOwnerAndSegFreeList() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(5), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            Optional<ExtentId> ext = seg.assignExtentToSegment(m, SPACE, slot);
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)), ext);
            assertEquals(ExtentState.FSEG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(Optional.of(SegmentId.of(5)), xdes.read(m, ExtentId.of(SPACE, 1)).ownerSegment());
            assertEquals(1L, flst.length(m, SPACE, inode.freeExtentListBaseAddr(SPACE, slot)));
            mgr.commit(m);
        });
    }

    @Test
    void allocatePageFromSegmentExtentMovesFreeToNotFullThenFull() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            seg.assignExtentToSegment(m, SPACE, slot);
            Optional<PageNo> p0 = seg.allocatePageFromSegmentExtents(m, SPACE, slot);
            assertEquals(Optional.of(PageNo.of(64)), p0);
            assertEquals(0L, flst.length(m, SPACE, inode.freeExtentListBaseAddr(SPACE, slot)));
            assertEquals(1L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            for (int i = 1; i < PE; i++) {
                assertEquals(Optional.of(PageNo.of(64 + i)), seg.allocatePageFromSegmentExtents(m, SPACE, slot));
            }
            assertEquals(0L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            assertEquals(1L, flst.length(m, SPACE, inode.fullExtentListBaseAddr(SPACE, slot)));
            assertEquals((long) PE, inode.read(m, SPACE, slot).usedPageCount());
            assertTrue(seg.allocatePageFromSegmentExtents(m, SPACE, slot).isEmpty());
            mgr.commit(m);
        });
    }

    @Test
    void freeFragmentPageClearsSlotAndRecyclesExtentWhenEmpty() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            PageNo p = seg.allocateFragmentPage(m, SPACE, slot).orElseThrow();
            assertEquals(PageNo.of(64), p);
            seg.freePage(m, SPACE, slot, PageId.of(SPACE, p));
            assertTrue(inode.getFragmentPage(m, SPACE, slot, 0).isEmpty());
            assertEquals(0L, inode.read(m, SPACE, slot).usedPageCount());
            assertEquals(ExtentState.FREE, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(1L, flst.length(m, SPACE, header.freeExtentListBaseAddr(SPACE)));
            assertEquals(0L, flst.length(m, SPACE, header.freeFragExtentListBaseAddr(SPACE)));
            mgr.commit(m);
        });
    }

    @Test
    void freeFragmentPageMovesFullFragBackToFreeFrag() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slotA = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            int slotB = inode.allocateSlot(a, SPACE, SegmentId.of(2), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            // 单段 fragment 槽仅 32，用两段把 64 页占满 extent1 → FULL_FRAG
            for (int i = 0; i < 32; i++) {
                seg.allocateFragmentPage(m, SPACE, slotA);
            }
            for (int i = 0; i < PE - 32; i++) {
                seg.allocateFragmentPage(m, SPACE, slotB);
            }
            assertEquals(ExtentState.FULL_FRAG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            // page 64 是 slotA 的首个 fragment 页；释放它 → FULL_FRAG 退回 FREE_FRAG
            seg.freePage(m, SPACE, slotA, PageId.of(SPACE, PageNo.of(64)));
            assertEquals(ExtentState.FREE_FRAG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(1L, flst.length(m, SPACE, header.freeFragExtentListBaseAddr(SPACE)));
            assertEquals(0L, flst.length(m, SPACE, header.fullFragExtentListBaseAddr(SPACE)));
            mgr.commit(m);
        });
    }

    @Test
    void freeSegmentExtentPageMovesFullToNotFullAndRecyclesWhenEmpty() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            seg.assignExtentToSegment(m, SPACE, slot);
            for (int i = 0; i < PE; i++) {
                seg.allocatePageFromSegmentExtents(m, SPACE, slot);
            }
            assertEquals(1L, flst.length(m, SPACE, inode.fullExtentListBaseAddr(SPACE, slot)));
            seg.freePage(m, SPACE, slot, PageId.of(SPACE, PageNo.of(64)));
            assertEquals(0L, flst.length(m, SPACE, inode.fullExtentListBaseAddr(SPACE, slot)));
            assertEquals(1L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            for (int i = 1; i < PE; i++) {
                seg.freePage(m, SPACE, slot, PageId.of(SPACE, PageNo.of(64 + i)));
            }
            assertEquals(0L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            assertEquals(ExtentState.FREE, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertTrue(xdes.read(m, ExtentId.of(SPACE, 1)).ownerSegment().isEmpty());
            assertEquals(1L, flst.length(m, SPACE, header.freeExtentListBaseAddr(SPACE)));
            assertEquals(0L, inode.read(m, SPACE, slot).usedPageCount());
            mgr.commit(m);
        });
    }

    @Test
    void freeRejectsSystemAndUnallocatedPages() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            assertThrows(FspMetadataException.class,
                    () -> seg.freePage(m, SPACE, slot, PageId.of(SPACE, PageNo.of(0))));
            assertThrows(FspMetadataException.class,
                    () -> seg.freePage(m, SPACE, slot, PageId.of(SPACE, PageNo.of(64))));
            mgr.commit(m);
        });
    }
}
