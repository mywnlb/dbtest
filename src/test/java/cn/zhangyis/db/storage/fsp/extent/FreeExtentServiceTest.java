package cn.zhangyis.db.storage.fsp.extent;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;


import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FreeExtentService 集成测试：freeLimit 跳过 extent0、按 extent 推进、currentSize 边界；acquire/return；
 * fragment 页分配（新建 FREE_FRAG、满迁 FULL_FRAG）。no-redo，不做 crash recovery 断言。
 */
class FreeExtentServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);
    private static final int PE = 64; // 16KB pagesPerExtent

    @TempDir
    Path dir;

    private SpaceHeaderSnapshot fresh(long sizePages) {
        return new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(sizePages), PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, 80046, 1L);
    }

    private interface Body {
        void run(SpaceHeaderRepository header, ExtentDescriptorRepository xdes, Flst flst,
                 FreeExtentService svc, MiniTransactionManager mgr, BufferPool pool);
    }

    private void withSvc(long sizePages, Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(sizePages));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 16)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, PS);
            Flst flst = new Flst(pool);
            FreeExtentService svc = new FreeExtentService(pool, PS, header, xdes, flst);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            header.initialize(init, fresh(sizePages));
            mgr.commit(init);
            body.run(header, xdes, flst, svc, mgr, pool);
        }
    }

    /**
     * 验证 {@code fillSkipsSystemExtentAndAdvancesByExtent} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void fillSkipsSystemExtentAndAdvancesByExtent() {
        withSvc(192, (header, xdes, flst, svc, mgr, pool) -> {
            MiniTransaction m = mgr.begin();
            Optional<ExtentId> first = svc.fillFreeListStep(m, SPACE);
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)), first); // extent0 skipped
            Optional<ExtentId> second = svc.fillFreeListStep(m, SPACE);
            assertEquals(Optional.of(ExtentId.of(SPACE, 2)), second);
            Optional<ExtentId> none = svc.fillFreeListStep(m, SPACE); // 192/64=3 extents, extent3 would exceed
            assertTrue(none.isEmpty());
            assertEquals(PageNo.of(192), header.read(m, SPACE).freeLimitPageNo());
            assertEquals(2L, flst.length(m, SPACE, header.freeExtentListBaseAddr(SPACE)));
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code acquireFillsThenPopsAndReturnsRecycle} 对应的表空间、区与段分配行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void acquireFillsThenPopsAndReturnsRecycle() {
        withSvc(192, (header, xdes, flst, svc, mgr, pool) -> {
            MiniTransaction m = mgr.begin();
            Optional<ExtentId> a = svc.acquireFreeExtent(m, SPACE);
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)), a);
            Optional<ExtentId> b = svc.acquireFreeExtent(m, SPACE);
            assertEquals(Optional.of(ExtentId.of(SPACE, 2)), b);
            Optional<ExtentId> c = svc.acquireFreeExtent(m, SPACE);
            assertTrue(c.isEmpty()); // exhausted
            svc.returnFreeExtent(m, SPACE, ExtentId.of(SPACE, 1));
            assertEquals(1L, flst.length(m, SPACE, header.freeExtentListBaseAddr(SPACE)));
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)), svc.acquireFreeExtent(m, SPACE));
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code acquireWithDirectionChoosesNearestFreeExtentAroundHint} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void acquireWithDirectionChoosesNearestFreeExtentAroundHint() {
        withSvc(320, (header, xdes, flst, svc, mgr, pool) -> {
            MiniTransaction m = mgr.begin();
            for (int i = 0; i < 4; i++) {
                svc.fillFreeListStep(m, SPACE);
            }

            assertEquals(Optional.of(ExtentId.of(SPACE, 2)),
                    svc.acquireFreeExtent(m, SPACE, ExtentAllocationDirection.UP, Optional.of(PageNo.of(128))));
            assertEquals(Optional.of(ExtentId.of(SPACE, 3)),
                    svc.acquireFreeExtent(m, SPACE, ExtentAllocationDirection.DOWN, Optional.of(PageNo.of(255))));
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)),
                    svc.acquireFreeExtent(m, SPACE, ExtentAllocationDirection.NO_DIRECTION, Optional.of(PageNo.of(255))));
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code upDirectionCanMaterializeHigherExtentBeforeFallingBack} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void upDirectionMaterializesAtMostOneExtentBeforeFallingBack() {
        withSvc(256, (header, xdes, flst, svc, mgr, pool) -> {
            MiniTransaction m = mgr.begin();
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)), svc.acquireFreeExtent(m, SPACE));
            assertEquals(Optional.of(ExtentId.of(SPACE, 2)),
                    svc.acquireFreeExtent(m, SPACE, ExtentAllocationDirection.UP, Optional.of(PageNo.of(192))));
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code allocateFragmentPagesFromFreeFragThenFull} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void allocateFragmentPagesFromFreeFragThenFull() {
        withSvc(128, (header, xdes, flst, svc, mgr, pool) -> {
            MiniTransaction m = mgr.begin();
            Optional<PageId> p0 = svc.allocateFragmentPage(m, SPACE);
            assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(64))), p0);
            assertEquals(ExtentState.FREE_FRAG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(1L, flst.length(m, SPACE, header.freeFragExtentListBaseAddr(SPACE)));
            for (int i = 1; i < PE; i++) {
                assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(64 + i))), svc.allocateFragmentPage(m, SPACE));
            }
            assertEquals(ExtentState.FULL_FRAG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(0L, flst.length(m, SPACE, header.freeFragExtentListBaseAddr(SPACE)));
            assertEquals(1L, flst.length(m, SPACE, header.fullFragExtentListBaseAddr(SPACE)));
            assertTrue(xdes.isFull(m, ExtentId.of(SPACE, 1)));
            assertTrue(svc.allocateFragmentPage(m, SPACE).isEmpty());
            mgr.commit(m);
        });
    }

    /**
     * 验证 page0 兼容槽耗尽后，分配链路会先格式化 group0 的 overflow XDES page5，再把第一个
     * 独立页 descriptor 挂入 FSP_FREE；该测试防止寻址公式存在但真实分配仍停在 page0 容量上限。
     */
    @Test
    void materializesFirstExtentBeyondPageZeroOnOverflowXdesPage() {
        long firstStandalone = ExtentDescriptorLayout.maxEntriesInPage0(PS);
        long freeLimit = firstStandalone * PS.pagesPerExtent();
        long logicalSize = freeLimit + PS.pagesPerExtent();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("overflow-xdes.ibd"), PS, PageNo.of(6));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, PS);
            Flst flst = new Flst(pool);
            FreeExtentService service = new FreeExtentService(pool, PS, header, xdes, flst);
            MiniTransactionManager manager = new MiniTransactionManager();
            MiniTransaction mtr = manager.begin();
            header.initialize(mtr, new SpaceHeaderSnapshot(SPACE, PS, 0,
                    PageNo.of(logicalSize), PageNo.of(freeLimit), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                    PageNo.of(2), 0L, 80046, 1L));
            xdes.reserveSystemExtent(mtr, SPACE);

            ExtentId legacyPageZeroExtent = ExtentId.of(SPACE, 1L);
            xdes.initFree(mtr, legacyPageZeroExtent);
            flst.addLast(mtr, SPACE, header.freeExtentListBaseAddr(SPACE),
                    xdes.listNodeAddr(legacyPageZeroExtent));
            ExtentId expected = ExtentId.of(SPACE, firstStandalone);
            assertEquals(Optional.of(expected), service.fillFreeListStep(mtr, SPACE));
            FileAddress first = flst.getFirst(mtr, SPACE, header.freeExtentListBaseAddr(SPACE));
            assertEquals(legacyPageZeroExtent, xdes.extentIdOfNode(SPACE, first));
            assertEquals(expected, xdes.extentIdOfNode(SPACE, flst.getNext(mtr, SPACE, first)));
            assertEquals(Optional.of(expected), service.acquireFreeExtent(mtr, SPACE,
                    ExtentAllocationDirection.UP,
                    Optional.of(PageNo.of(firstStandalone * PS.pagesPerExtent()))));
            assertEquals(1L, flst.length(mtr, SPACE, header.freeExtentListBaseAddr(SPACE)));
            assertEquals(ExtentState.FREE, xdes.read(mtr, expected).state());
            PageGuard page5 = mtr.getPage(pool, PageId.of(SPACE, PageNo.of(5)), PageLatchMode.SHARED);
            assertEquals(PageType.XDES, PageEnvelope.readHeader(page5).pageType());
            assertTrue(xdes.isPageAllocated(mtr, ExtentId.of(SPACE, 0), 5));
            manager.commit(mtr);
            assertTrue(manager.redoLogManager().bufferedRecords().stream()
                    .filter(PageInitRecord.class::isInstance)
                    .map(PageInitRecord.class::cast)
                    .anyMatch(record -> record.pageId().equals(PageId.of(SPACE, PageNo.of(5)))
                            && record.pageType() == PageType.XDES));
        }
    }

    /**
     * 验证 group0 延迟启用 page5 前会先复核 extent0 的系统 owner 形状；若旧文件已经把该 descriptor
     * 改成普通 segment 所有权，在线升级必须保留全零 page5，不能留下 MTR 回滚无法撤销的半格式管理页。
     */
    @Test
    void rejectsConflictingExtentZeroBeforeFormattingGroupZeroOverflow() {
        long firstStandalone = ExtentDescriptorLayout.maxEntriesInPage0(PS);
        long freeLimit = firstStandalone * PS.pagesPerExtent();
        long logicalSize = freeLimit + PS.pagesPerExtent();
        PageId page5Id = PageId.of(SPACE, PageNo.of(5));
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("group-zero-owner-conflict.ibd"), PS, PageNo.of(6));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, PS);
            FreeExtentService service = new FreeExtentService(pool, PS, header, xdes, new Flst(pool));
            MiniTransactionManager manager = new MiniTransactionManager();
            MiniTransaction setup = manager.begin();
            header.initialize(setup, new SpaceHeaderSnapshot(SPACE, PS, 0,
                    PageNo.of(logicalSize), PageNo.of(freeLimit), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                    PageNo.of(2), 0L, 80046, 1L));
            xdes.reserveSystemExtent(setup, SPACE);
            PageGuard page0 = setup.getPage(pool, PageId.of(SPACE, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
            page0.writeLong(ExtentDescriptorLayout.entryOffset(0L)
                    + ExtentDescriptorLayout.OWNER_SEGMENT, 99L);
            manager.commit(setup);

            MiniTransaction materialize = manager.begin();
            assertThrows(cn.zhangyis.db.storage.fsp.exception.FspMetadataException.class,
                    () -> service.fillFreeListStep(materialize, SPACE));
            manager.rollbackUncommitted(materialize);

            MiniTransaction verify = manager.beginReadOnly();
            for (byte value : verify.getPage(pool, page5Id, PageLatchMode.SHARED).readBytes(0, PS.bytes())) {
                assertEquals(0, value, "extent0 conflict must be rejected before page5 formatting");
            }
            manager.commit(verify);
        }
    }

    /**
     * 验证跨越重复管理区边界时，区首 extent 不进入普通 FREE 链，并且 primary XDES 与 +1
     * IBUF_BITMAP 在返回下一普通 extent 前完成格式化和固定页位图保留。
     */
    @Test
    void reservesManagementExtentAndFormatsRepeatedBitmapBeforeAllocation() {
        PageSize pageSize = PageSize.ofBytes(4 * 1024);
        ExtentManagementRegionLayout layout = new ExtentManagementRegionLayout(pageSize);
        long managementExtent = layout.extentsPerManagementRegion();
        long freeLimit = managementExtent * pageSize.pagesPerExtent();
        long logicalSize = freeLimit + pageSize.pagesPerExtent() * 2L;
        long bitmapPageNo = layout.bitmapPageNo(1).value();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("repeated-bitmap.ibd"), pageSize, PageNo.of(bitmapPageNo + 1));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, pageSize, 8)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, pageSize);
            Flst flst = new Flst(pool);
            FreeExtentService service = new FreeExtentService(pool, pageSize, header, xdes, flst);
            MiniTransactionManager manager = new MiniTransactionManager();
            MiniTransaction mtr = manager.begin();
            header.initialize(mtr, new SpaceHeaderSnapshot(SPACE, pageSize, 0,
                    PageNo.of(logicalSize), PageNo.of(freeLimit), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                    PageNo.of(2), 0L, 80046, 1L));

            ExtentId ordinary = ExtentId.of(SPACE, managementExtent + 1L);
            assertEquals(Optional.of(ordinary), service.fillFreeListStep(mtr, SPACE));
            ExtentId reserved = ExtentId.of(SPACE, managementExtent);
            assertEquals(ExtentState.FSEG_FRAG, xdes.read(mtr, reserved).state());
            assertTrue(xdes.isPageAllocated(mtr, reserved, 0));
            assertTrue(xdes.isPageAllocated(mtr, reserved, 1));
            assertEquals(1L, flst.length(mtr, SPACE, header.freeExtentListBaseAddr(SPACE)));
            PageGuard primary = mtr.getPage(pool, PageId.of(SPACE, layout.primaryXdesPageNo(1)),
                    PageLatchMode.SHARED);
            PageGuard bitmap = mtr.getPage(pool, PageId.of(SPACE, layout.bitmapPageNo(1)),
                    PageLatchMode.SHARED);
            assertEquals(PageType.XDES, PageEnvelope.readHeader(primary).pageType());
            assertEquals(PageType.IBUF_BITMAP, PageEnvelope.readHeader(bitmap).pageType());
            manager.commit(mtr);
            assertTrue(manager.redoLogManager().bufferedRecords().stream()
                    .filter(PageInitRecord.class::isInstance)
                    .map(PageInitRecord.class::cast)
                    .anyMatch(record -> record.pageId().equals(PageId.of(SPACE, layout.bitmapPageNo(1)))
                            && record.pageType() == PageType.IBUF_BITMAP));
        }
    }

    /**
     * 验证 legacy 文件若已把未来 bitmap 固定位置写成业务内容，在线跨区会保留证据并 fail-closed；由于 MTR
     * 不提供 content undo，initializer 必须完成整组预检后才格式化前面的 primary 页。
     */
    @Test
    void rejectsConflictingLegacyManagementPageBeforeFormattingAnyPeer() {
        PageSize pageSize = PageSize.ofBytes(4 * 1024);
        ExtentManagementRegionLayout layout = new ExtentManagementRegionLayout(pageSize);
        long managementExtent = layout.extentsPerManagementRegion();
        long freeLimit = managementExtent * pageSize.pagesPerExtent();
        long logicalSize = freeLimit + pageSize.pagesPerExtent() * 2L;
        PageId primaryId = PageId.of(SPACE, layout.primaryXdesPageNo(1));
        PageId bitmapId = PageId.of(SPACE, layout.bitmapPageNo(1));
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("legacy-management-conflict.ibd"), pageSize,
                PageNo.of(bitmapId.pageNo().value() + 1L));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, pageSize, 8)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, pageSize);
            FreeExtentService service = new FreeExtentService(pool, pageSize, header, xdes, new Flst(pool));
            MiniTransactionManager manager = new MiniTransactionManager();
            MiniTransaction setup = manager.begin();
            header.initialize(setup, new SpaceHeaderSnapshot(SPACE, pageSize, 0,
                    PageNo.of(logicalSize), PageNo.of(freeLimit), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                    PageNo.of(2), 0L, 80046, 1L));
            setup.getPage(pool, bitmapId, PageLatchMode.EXCLUSIVE).writeBytes(100, new byte[]{0x55});
            manager.commit(setup);

            MiniTransaction materialize = manager.begin();
            assertThrows(cn.zhangyis.db.storage.fsp.exception.FspMetadataException.class,
                    () -> service.fillFreeListStep(materialize, SPACE));
            manager.rollbackUncommitted(materialize);

            MiniTransaction verify = manager.beginReadOnly();
            byte[] primaryPrefix = verify.getPage(pool, primaryId, PageLatchMode.SHARED).readBytes(0, 80);
            for (byte value : primaryPrefix) {
                assertEquals(0, value, "conflict preflight must not partially format primary XDES");
            }
            assertEquals(0x55, Byte.toUnsignedInt(
                    verify.getPage(pool, bitmapId, PageLatchMode.SHARED).readBytes(100, 1)[0]));
            manager.commit(verify);
        }
    }

    /**
     * 验证 4KB/8KB 兼容布局中仍位于 page0 的未来管理 extent 若已有 legacy owner，升级会在读取远端
     * primary/bitmap 之前拒绝，不能以“固定页迁移”为名覆盖旧业务归属。
     */
    @Test
    void rejectsLegacyOwnerOnFutureManagementExtentBeforeFormattingPages() {
        PageSize pageSize = PageSize.ofBytes(4 * 1024);
        ExtentManagementRegionLayout layout = new ExtentManagementRegionLayout(pageSize);
        long managementExtent = layout.extentsPerManagementRegion();
        long freeLimit = managementExtent * pageSize.pagesPerExtent();
        long logicalSize = freeLimit + pageSize.pagesPerExtent() * 2L;
        PageId primaryId = PageId.of(SPACE, layout.primaryXdesPageNo(1));
        PageId bitmapId = PageId.of(SPACE, layout.bitmapPageNo(1));
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("legacy-owner-conflict.ibd"), pageSize,
                PageNo.of(bitmapId.pageNo().value() + 1L));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, pageSize, 8)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, pageSize);
            FreeExtentService service = new FreeExtentService(pool, pageSize, header, xdes, new Flst(pool));
            MiniTransactionManager manager = new MiniTransactionManager();
            MiniTransaction setup = manager.begin();
            header.initialize(setup, new SpaceHeaderSnapshot(SPACE, pageSize, 0,
                    PageNo.of(logicalSize), PageNo.of(freeLimit), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                    PageNo.of(2), 0L, 80046, 1L));
            PageGuard page0 = setup.getPage(pool, PageId.of(SPACE, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
            page0.writeLong(ExtentDescriptorLayout.entryOffset(managementExtent)
                    + ExtentDescriptorLayout.OWNER_SEGMENT, 99L);
            manager.commit(setup);

            MiniTransaction materialize = manager.begin();
            assertThrows(cn.zhangyis.db.storage.fsp.exception.FspMetadataException.class,
                    () -> service.fillFreeListStep(materialize, SPACE));
            manager.rollbackUncommitted(materialize);

            MiniTransaction verify = manager.beginReadOnly();
            for (PageId pageId : new PageId[]{primaryId, bitmapId}) {
                byte[] prefix = verify.getPage(pool, pageId, PageLatchMode.SHARED).readBytes(0, 80);
                for (byte value : prefix) {
                    assertEquals(0, value, "legacy owner conflict must preserve all remote management pages");
                }
            }
            manager.commit(verify);
        }
    }
}
