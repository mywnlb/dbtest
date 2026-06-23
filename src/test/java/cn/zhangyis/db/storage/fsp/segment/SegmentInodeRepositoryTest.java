package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLayout;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SegmentInodeRepository 集成测试：allocateSlot→read（三 list 头为 FlstBase 空链）、segmentId 0 拒绝、
 * freeSlot 清零复用、读空槽拒绝、标量/fragment setter、坏 purpose ordinal 拒绝，以及 base 访问器经 Flst 跨页维护。
 */
class SegmentInodeRepositoryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private interface Body {
        void run(SegmentInodeRepository repo, MiniTransaction mtr);
    }

    private void withRepo(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SegmentInodeRepository repo = new SegmentInodeRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            body.run(repo, mtr);
            mgr.commit(mtr);
        }
    }

    @Test
    void allocateThenReadRoundTrips() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(3), SegmentPurpose.INDEX_LEAF);
            assertEquals(0, slot);
            SegmentInode inode = repo.read(mtr, SPACE, slot);
            assertEquals(SegmentId.of(3), inode.segmentId());
            assertEquals(SegmentPurpose.INDEX_LEAF, inode.purpose());
            assertEquals(0L, inode.usedPageCount());
            assertEquals(FlstBase.EMPTY, inode.freeExtentList());
            assertEquals(FlstBase.EMPTY, inode.notFullExtentList());
            assertEquals(FlstBase.EMPTY, inode.fullExtentList());
        });
    }

    @Test
    void shouldRejectSegmentIdZero() {
        withRepo((repo, mtr) ->
                assertThrows(DatabaseValidationException.class,
                        () -> repo.allocateSlot(mtr, SPACE, SegmentId.of(0), SegmentPurpose.SYSTEM)));
    }

    @Test
    void freeSlotAllowsReuseAndReadingFreeSlotThrows() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(1), SegmentPurpose.UNDO);
            repo.setUsedPageCount(mtr, SPACE, slot, 5L);
            repo.setFragmentPage(mtr, SPACE, slot, 0, Optional.of(PageNo.of(40)));
            repo.freeSlot(mtr, SPACE, slot);
            assertThrows(FspMetadataException.class, () -> repo.read(mtr, SPACE, slot));
            int reused = repo.allocateSlot(mtr, SPACE, SegmentId.of(2), SegmentPurpose.SYSTEM);
            assertEquals(slot, reused);
            assertTrue(repo.getFragmentPage(mtr, SPACE, reused, 0).isEmpty());
        });
    }

    @Test
    void scalarSettersAndFragmentSlots() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(9), SegmentPurpose.INDEX_NON_LEAF);
            repo.setUsedPageCount(mtr, SPACE, slot, 7L);
            repo.setReservedPageCount(mtr, SPACE, slot, 3L);
            assertEquals(0, repo.requireFreeFragmentSlot(mtr, SPACE, slot));
            repo.setFragmentPage(mtr, SPACE, slot, 0, Optional.of(PageNo.of(40)));
            assertEquals(Optional.of(PageNo.of(40)), repo.getFragmentPage(mtr, SPACE, slot, 0));
            assertEquals(1, repo.requireFreeFragmentSlot(mtr, SPACE, slot));

            SegmentInode inode = repo.read(mtr, SPACE, slot);
            assertEquals(7L, inode.usedPageCount());
            assertEquals(3L, inode.reservedPageCount());
        });
    }

    @Test
    void fullFragmentSlotsAndPageZeroThrow() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(1), SegmentPurpose.LOB);
            assertThrows(DatabaseValidationException.class,
                    () -> repo.setFragmentPage(mtr, SPACE, slot, 0, Optional.of(PageNo.of(0))));
            for (int i = 0; i < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; i++) {
                repo.setFragmentPage(mtr, SPACE, slot, i, Optional.of(PageNo.of(40 + i)));
            }
            assertThrows(FspMetadataException.class, () -> repo.requireFreeFragmentSlot(mtr, SPACE, slot));
        });
    }

    @Test
    void badFragmentIndexThrows() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(1), SegmentPurpose.LOB);
            assertThrows(DatabaseValidationException.class,
                    () -> repo.getFragmentPage(mtr, SPACE, slot, 32));
        });
    }

    @Test
    void badPurposeOrdinalThrowsMetadataException() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SegmentInodeRepository repo = new SegmentInodeRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            int slot = repo.allocateSlot(init, SPACE, SegmentId.of(1), SegmentPurpose.LOB);
            mgr.commit(init);

            MiniTransaction corrupt = mgr.begin();
            PageGuard g = corrupt.getPage(pool, PageId.of(SPACE, PageNo.of(2)), PageLatchMode.EXCLUSIVE);
            g.writeInt(SegmentInodeLayout.slotOffset(slot) + SegmentInodeLayout.PURPOSE, 99);
            mgr.commit(corrupt);

            MiniTransaction r = mgr.begin();
            assertThrows(FspMetadataException.class, () -> repo.read(r, SPACE, slot));
            mgr.commit(r);
        }
    }

    @Test
    void segmentExtentListBaseManagedByFlstCrossPage() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SegmentInodeRepository repo = new SegmentInodeRepository(pool, PS);
            Flst flst = new Flst(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();

            // allocateSlot 取 page2；与跨页 addLast（page0→page2）用 commit 分隔，避免 page2 先于 page0 的逆序。
            MiniTransaction a = mgr.begin();
            int slot = repo.allocateSlot(a, SPACE, SegmentId.of(3), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction w = mgr.begin();
            FileAddress base = repo.notFullExtentListBaseAddr(SPACE, slot);   // page2
            FileAddress node = FileAddress.of(PageNo.of(0),
                    ExtentDescriptorLayout.entryOffset(2) + ExtentDescriptorLayout.PREV); // page0
            flst.addLast(w, SPACE, base, node);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            SegmentInode inode = repo.read(r, SPACE, slot);
            mgr.commit(r);
            assertEquals(1L, inode.notFullExtentList().length());
            assertEquals(node, inode.notFullExtentList().first());
            assertEquals(node, inode.notFullExtentList().last());
        }
    }

    @Test
    void hasFreeFragmentSlotReflectsUsage() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            assertTrue(repo.hasFreeFragmentSlot(mtr, SPACE, slot));
            for (int i = 0; i < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; i++) {
                repo.setFragmentPage(mtr, SPACE, slot, i, Optional.of(PageNo.of(40 + i)));
            }
            assertFalse(repo.hasFreeFragmentSlot(mtr, SPACE, slot));
        });
    }
}
