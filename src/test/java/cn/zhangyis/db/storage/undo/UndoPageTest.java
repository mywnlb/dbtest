package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.FilePageHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3b UndoPage：验证 first/chain 页格式、header 拆分初值、append 只推进 page header、FIL 链接、
 * first-only log header 守门，以及错误页类型拒绝。
 */
class UndoPageTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir
    Path dir;

    @Test
    void formatFirstPageInitsBothHeaders() {
        onFirstPage((page, handle) -> {
            assertEquals(97, page.freeOffset());
            assertEquals(0, page.recordCount());
            assertEquals(0L, page.pageLastUndoNo().value());
            assertTrue(page.isFirstPage());
            assertEquals(handle.segmentId().value(), page.segmentId().value());
            assertEquals(handle.inodeSlot(), page.inodeSlot());
            assertEquals(UndoLogKind.INSERT, page.undoKind());
            assertEquals(7L, page.transactionId().value());
            assertEquals(page.pageId().pageNo().value(), page.firstPageNo());
            assertEquals(page.pageId().pageNo().value(), page.lastPageNo());
            assertEquals(0L, page.logRecordCount());
            assertEquals(0L, page.logLastUndoNo().value());
        });
    }

    @Test
    void appendAdvancesPageHeaderOnlyAndReadsBack() {
        onFirstPage((page, handle) -> {
            byte[] a = {1, 2, 3};
            int offA = page.appendRecord(a, UndoNo.of(1));
            byte[] b = {9, 9};
            int offB = page.appendRecord(b, UndoNo.of(2));
            assertEquals(97, offA);
            assertEquals(97 + 2 + 3, offB);
            assertEquals(2, page.recordCount());
            assertEquals(2L, page.pageLastUndoNo().value());
            assertArrayEquals(a, page.recordAt(offA));
            assertArrayEquals(b, page.recordAt(offB));
            assertEquals(0L, page.logRecordCount());
            assertEquals(0L, page.logLastUndoNo().value());
        });
    }

    @Test
    void appendRejectsNoneUndoNo() {
        onFirstPage((page, handle) ->
                assertThrows(DatabaseValidationException.class,
                        () -> page.appendRecord(new byte[]{1}, UndoNo.NONE)));
    }

    @Test
    void appendOverflowThrows() {
        onFirstPage((page, handle) ->
                assertThrows(UndoPageOverflowException.class,
                        () -> page.appendRecord(new byte[PS.bytes()], UndoNo.of(1))));
    }

    @Test
    void recordAtRejectsOutOfArea() {
        onFirstPage((page, handle) -> {
            page.appendRecord(new byte[]{1, 2}, UndoNo.of(1));
            assertThrows(UndoLogFormatException.class, () -> page.recordAt(10_000));
        });
    }

    @Test
    void chainPageIsNotFirstAndLogHeaderAccessorsThrow() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId p1 = disk.allocatePage(m, seg);
            PageId p2 = disk.allocatePage(m, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), p1, p1);
            undoAccess.createFirstPage(m, p1, UndoLogKind.INSERT, TransactionId.of(7), handle);
            UndoPage chain = undoAccess.createChainPage(m, p2, handle);
            assertFalse(chain.isFirstPage());
            assertEquals(97, chain.freeOffset());
            assertEquals(handle.segmentId().value(), chain.segmentId().value());
            assertThrows(UndoLogFormatException.class, chain::transactionId);
            assertThrows(UndoLogFormatException.class, chain::undoKind);
            assertThrows(UndoLogFormatException.class, chain::logRecordCount);
            assertThrows(UndoLogFormatException.class, () -> chain.setLastPageNo(PageNo.of(5)));
            mgr.commit(m);
        });
    }

    @Test
    void linkNextPreservesPrevAndViceVersa() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId p1 = disk.allocatePage(m, seg);
            PageId p2 = disk.allocatePage(m, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), p1, p1);
            UndoPage first = undoAccess.createFirstPage(m, p1, UndoLogKind.INSERT, TransactionId.of(7), handle);
            UndoPage chain = undoAccess.createChainPage(m, p2, handle);
            first.linkNextTo(p2.pageNo());
            chain.linkPrevTo(p1.pageNo());
            assertEquals(p2.pageNo().value(), first.nextPageNo());
            assertEquals(FilePageHeader.FIL_NULL, first.prevPageNo());
            assertEquals(p1.pageNo().value(), chain.prevPageNo());
            assertEquals(FilePageHeader.FIL_NULL, chain.nextPageNo());
            mgr.commit(m);
        });
    }

    @Test
    void openUndoPageRejectsAllocatedType() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(m, seg);
            mgr.commit(m);

            MiniTransaction r = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> undoAccess.openUndoPage(r, pid, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(r);
        });
    }

    @Test
    void openUndoPageRejectsIndexType() {
        onPool((mgr, disk, undoAccess, pool) -> {
            IndexPageAccess idx = new IndexPageAccess(pool, PS);
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.INDEX_LEAF);
            PageId pid = disk.allocatePage(m, seg);
            idx.createIndexPage(m, pid, 1L, 0);
            mgr.commit(m);

            MiniTransaction r = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> undoAccess.openUndoPage(r, pid, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(r);
        });
    }

    private interface FirstPageBody {
        void run(UndoPage page, UndoSegmentHandle handle);
    }

    private interface PoolBody {
        void run(MiniTransactionManager mgr, DiskSpaceManager disk, UndoPageAccess undoAccess, BufferPool pool);
    }

    private void onFirstPage(FirstPageBody body) {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(m, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), pid, pid);
            UndoPage page = undoAccess.createFirstPage(m, pid, UndoLogKind.INSERT, TransactionId.of(7), handle);
            body.run(page, handle);
            mgr.commit(m);
        });
    }

    private void onPool(PoolBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoPageAccess undoAccess = new UndoPageAccess(pool, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, disk, undoAccess, pool);
        }
    }
}
