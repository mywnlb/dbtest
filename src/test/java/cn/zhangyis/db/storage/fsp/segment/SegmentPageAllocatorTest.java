package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.extent.DefaultExtentAllocationPolicy;
import cn.zhangyis.db.storage.fsp.extent.ExtentAllocationPolicy;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.extent.FreeExtentService;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;


import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
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
 * SegmentPageAllocator 集成测试：fragment 路径（前 32 页）、满 32 转 extent 路径、无空间返回 empty、policy 多 extent。
 */
class SegmentPageAllocatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private SpaceHeaderSnapshot fresh(long sizePages) {
        return new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(sizePages), PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, 80046, 1L);
    }

    interface Ctx {
        void run(SpaceHeaderRepository header, SegmentInodeRepository inode, Flst flst,
                 SegmentPageAllocator alloc, MiniTransactionManager mgr);
    }

    private void withAlloc(long sizePages, ExtentAllocationPolicy policy, Ctx body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(sizePages));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 16)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, PS);
            SegmentInodeRepository inode = new SegmentInodeRepository(pool, PS);
            Flst flst = new Flst(pool);
            FreeExtentService free = new FreeExtentService(pool, PS, header, xdes, flst);
            SegmentSpaceService seg = new SegmentSpaceService(pool, PS, header, inode, xdes, flst, free);
            SegmentPageAllocator alloc = new SegmentPageAllocator(pool, inode, flst, seg, policy);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            header.initialize(init, fresh(sizePages));
            mgr.commit(init);
            body.run(header, inode, flst, alloc, mgr);
        }
    }

    @Test
    void fragmentFirst32ThenExtentPath() {
        withAlloc(192, new DefaultExtentAllocationPolicy(), (header, inode, flst, alloc, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            for (int i = 0; i < 32; i++) {
                assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(64 + i))),
                        alloc.allocatePage(m, SPACE, slot));
            }
            assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(128))), alloc.allocatePage(m, SPACE, slot));
            mgr.commit(m);
        });
    }

    @Test
    void returnsEmptyWhenNoSpaceAndNoAutoextend() {
        withAlloc(64, new DefaultExtentAllocationPolicy(), (header, inode, flst, alloc, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            assertTrue(alloc.allocatePage(m, SPACE, slot).isEmpty());
            mgr.commit(m);
        });
    }

    @Test
    void honorsPolicyAcquiringMultipleExtents() {
        ExtentAllocationPolicy two = ownedExtentCount -> 2;
        withAlloc(256, two, (header, inode, flst, alloc, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            for (int i = 0; i < 32; i++) {
                alloc.allocatePage(m, SPACE, slot);
            }
            assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(128))), alloc.allocatePage(m, SPACE, slot));
            assertEquals(1L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            assertEquals(1L, flst.length(m, SPACE, inode.freeExtentListBaseAddr(SPACE, slot)));
            mgr.commit(m);
        });
    }
}
