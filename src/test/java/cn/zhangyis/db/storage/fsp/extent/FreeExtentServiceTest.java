package cn.zhangyis.db.storage.fsp.extent;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;


import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
