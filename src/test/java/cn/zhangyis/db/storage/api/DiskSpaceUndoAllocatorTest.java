package cn.zhangyis.db.storage.api;

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
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3b undo 分配适配器：api 层把 FSP segment 分配映射为 undo 自有 handle，不向 undo 暴露 SegmentRef。
 */
class DiskSpaceUndoAllocatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir
    Path dir;

    @Test
    void createUndoSegmentReturnsHandleWithFirstPage() {
        onPool((mgr, allocator) -> {
            MiniTransaction m = mgr.begin();
            UndoSegmentHandle h = allocator.createUndoSegment(m, UNDO_SPACE);
            assertEquals(UNDO_SPACE, h.spaceId());
            assertTrue(h.inodeSlot() >= 0);
            assertTrue(h.segmentId().value() > 0);
            assertEquals(UNDO_SPACE, h.firstPageId().spaceId());
            assertEquals(h.firstPageId(), h.lastPageId());
            mgr.commit(m);
        });
    }

    @Test
    void allocatePageGivesDistinctPageInSameSegment() {
        onPool((mgr, allocator) -> {
            MiniTransaction m = mgr.begin();
            UndoSegmentHandle h = allocator.createUndoSegment(m, UNDO_SPACE);
            PageId p2 = allocator.allocatePage(m, UNDO_SPACE, h.inodeSlot(), h.segmentId());
            assertEquals(UNDO_SPACE, p2.spaceId());
            assertNotEquals(h.firstPageId().pageNo(), p2.pageNo());
            PageId p3 = allocator.allocatePage(m, UNDO_SPACE, h.inodeSlot(), h.segmentId());
            assertNotEquals(p2.pageNo(), p3.pageNo());
            mgr.commit(m);
        });
    }

    private interface PoolBody {
        void run(MiniTransactionManager mgr, DiskSpaceUndoAllocator allocator);
    }

    private void onPool(PoolBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, allocator);
        }
    }
}
