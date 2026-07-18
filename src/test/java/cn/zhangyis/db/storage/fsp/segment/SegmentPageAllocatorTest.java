package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.extent.DefaultExtentAllocationPolicy;
import cn.zhangyis.db.storage.fsp.extent.ExtentAllocationDirection;
import cn.zhangyis.db.storage.fsp.extent.ExtentAllocationPolicy;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.extent.FreeExtentService;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
            SegmentPageAllocator alloc = new SegmentPageAllocator(pool, PS, header, inode, flst, seg, policy);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            header.initialize(init, fresh(sizePages));
            mgr.commit(init);
            body.run(header, inode, flst, alloc, mgr);
        }
    }

    /**
     * 验证 {@code fragmentFirst32ThenExtentPath} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
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

    /**
     * 验证 {@code returnsEmptyWhenNoSpaceAndNoAutoextend} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
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

    /**
     * 验证 {@code directionalAllocationRequiresHintBeforeFragmentPath} 对应的表空间、区与段分配行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void directionalAllocationRequiresHintBeforeFragmentPath() {
        withAlloc(192, new DefaultExtentAllocationPolicy(), (header, inode, flst, alloc, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            assertThrows(DatabaseValidationException.class,
                    () -> alloc.allocatePage(m, SPACE, slot, ExtentAllocationDirection.UP, Optional.empty(), 1L));
            mgr.rollbackUncommitted(m);
        });
    }

    /**
     * 验证 {@code honorsPolicyAcquiringMultipleExtents} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void honorsPolicyAcquiringMultipleExtents() {
        ExtentAllocationPolicy two = request -> 2;
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

    /**
     * 验证 {@code noHintKeepsDefaultPolicyToSingleExtentEvenForLargeLeafSegment} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void noHintKeepsDefaultPolicyToSingleExtentEvenForLargeLeafSegment() {
        withAlloc(384, new DefaultExtentAllocationPolicy(), (header, inode, flst, alloc, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            for (int i = 0; i < 32 + 64 + 64; i++) {
                alloc.allocatePage(m, SPACE, slot);
            }
            assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(256))), alloc.allocatePage(m, SPACE, slot));
            assertEquals(1L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            assertEquals(0L, flst.length(m, SPACE, inode.freeExtentListBaseAddr(SPACE, slot)));
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code directionalLeafGrowthCanAssignMultipleExtents} 所描述的 B+Tree 定位或结构变化，并断言键序、父子链接、页资源和唯一性不变量。
     */
    @Test
    void directionalLeafGrowthCanAssignMultipleExtents() {
        withAlloc(384, new DefaultExtentAllocationPolicy(), (header, inode, flst, alloc, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            for (int i = 0; i < 32 + 64 + 64; i++) {
                alloc.allocatePage(m, SPACE, slot, ExtentAllocationDirection.UP,
                        Optional.of(PageNo.of(128)), 1L);
            }
            assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(256))),
                    alloc.allocatePage(m, SPACE, slot, ExtentAllocationDirection.UP,
                            Optional.of(PageNo.of(192)), PS.pagesPerExtent() * 2L));
            assertEquals(1L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            assertEquals(1L, flst.length(m, SPACE, inode.freeExtentListBaseAddr(SPACE, slot)));
            mgr.commit(m);
        });
    }
}
