package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.exception.TablespaceUnavailableException;
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.meta.CachingTablespaceRegistry;
import cn.zhangyis.db.storage.fil.meta.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.fil.state.SpaceFlags;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.FilePageHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

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
            assertEquals(136, UndoPageLayout.RECORD_AREA_START,
                    "v3 page format reserves logical head plus persistent history links");
            assertEquals(120, UndoPageLayout.HISTORY_PREV_PAGE_NO);
            assertEquals(128, UndoPageLayout.HISTORY_NEXT_PAGE_NO);
            assertEquals(3, UndoPageLayout.CURRENT_FORMAT_VERSION);
            assertEquals(UndoPageLayout.RECORD_AREA_START, page.freeOffset());
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
            assertEquals(UndoLogicalHead.EMPTY, page.logicalHead());
            assertEquals(cn.zhangyis.db.storage.page.FilePageHeader.FIL_NULL, page.historyPrevPageNo());
            assertEquals(cn.zhangyis.db.storage.page.FilePageHeader.FIL_NULL, page.historyNextPageNo());
            assertEquals(UndoPageLayout.CURRENT_FORMAT_VERSION, page.formatVersion());
        });
    }

    @Test
    void appendAdvancesPageHeaderOnlyAndReadsBack() {
        onFirstPage((page, handle) -> {
            byte[] a = {1, 2, 3};
            int offA = page.appendRecord(a, TransactionId.of(7), UndoNo.of(1));
            byte[] b = {9, 9};
            int offB = page.appendRecord(b, TransactionId.of(7), UndoNo.of(2));
            assertEquals(UndoPageLayout.RECORD_AREA_START, offA);
            assertEquals(UndoPageLayout.RECORD_AREA_START + 2 + 3, offB);
            assertEquals(2, page.recordCount());
            assertEquals(2L, page.pageLastUndoNo().value());
            assertArrayEquals(a, page.recordAt(offA));
            assertArrayEquals(b, page.recordAt(offB));
            assertEquals(0L, page.logRecordCount());
            assertEquals(0L, page.logLastUndoNo().value());
        });
    }

    /** logical undo 头必须把 undoNo 与 RollPointer 作为一个不可拆分的持久边界读写。 */
    @Test
    void logicalHeadRoundTripsAsOnePair() {
        onFirstPage((page, handle) -> {
            RollPointer pointer = new RollPointer(false, page.pageId().pageNo(),
                    UndoPageLayout.RECORD_AREA_START);
            UndoLogicalHead head = new UndoLogicalHead(UndoNo.of(9), pointer);

            page.setLogicalHead(head);

            assertEquals(head, page.logicalHead());
        });
    }

    /** 一空一非空会让 recovery 从错误边界开始，值对象必须在写页前拒绝该状态。 */
    @Test
    void logicalHeadRejectsInconsistentPair() {
        assertThrows(DatabaseValidationException.class,
                () -> new UndoLogicalHead(UndoNo.NONE,
                        new RollPointer(false, PageNo.of(9), UndoPageLayout.RECORD_AREA_START)));
        assertThrows(DatabaseValidationException.class,
                () -> new UndoLogicalHead(UndoNo.of(1), RollPointer.NULL));
    }

    @Test
    void appendRejectsNoneUndoNo() {
        onFirstPage((page, handle) ->
                assertThrows(DatabaseValidationException.class,
                        () -> page.appendRecord(new byte[]{1}, TransactionId.of(7), UndoNo.NONE)));
    }

    @Test
    void appendOverflowThrows() {
        onFirstPage((page, handle) ->
                assertThrows(UndoPageOverflowException.class,
                        () -> page.appendRecord(new byte[PS.bytes()], TransactionId.of(7), UndoNo.of(1))));
    }

    @Test
    void recordAtRejectsOutOfArea() {
        onFirstPage((page, handle) -> {
            page.appendRecord(new byte[]{1, 2}, TransactionId.of(7), UndoNo.of(1));
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
            UndoPage chain = undoAccess.createChainPage(m, p2, UndoLogKind.INSERT, handle);
            assertFalse(chain.isFirstPage());
            assertEquals(UndoPageLayout.RECORD_AREA_START, chain.freeOffset());
            assertEquals(UndoPageLayout.CURRENT_FORMAT_VERSION, chain.formatVersion());
            assertEquals(handle.segmentId().value(), chain.segmentId().value());
            assertThrows(UndoLogFormatException.class, chain::transactionId);
            assertEquals(UndoLogKind.INSERT, chain.undoKind(),
                    "v3 copies the authoritative log kind to every ordinary undo page");
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
            UndoPage chain = undoAccess.createChainPage(m, p2, UndoLogKind.INSERT, handle);
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

    /** 扩展 record-area 后不能把旧 flags 的页按新偏移静默解析，必须在打开入口 fail-closed。 */
    @Test
    void openUndoPageRejectsLegacyFormatVersion() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction create = mgr.begin();
            SegmentRef seg = disk.createSegment(create, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(create, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(
                    UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), pid, pid);
            undoAccess.createFirstPage(create, pid, UndoLogKind.INSERT, TransactionId.of(7), handle);
            mgr.commit(create);

            MiniTransaction corrupt = mgr.begin();
            int legacyV1Flags = (1 << UndoPageLayout.FORMAT_VERSION_SHIFT) | UndoPageLayout.FLAG_FIRST_PAGE;
            corrupt.getPage(pool, pid, PageLatchMode.EXCLUSIVE).writeBytes(
                    UndoPageLayout.PAGE_FLAGS, new byte[]{(byte) legacyV1Flags});
            mgr.commit(corrupt);

            MiniTransaction read = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> undoAccess.openUndoPage(read, pid, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(read);
        });
    }

    /** v2 的 record area 从 120 开始，不能被 v3 当成带 history links 的页面打开。 */
    @Test
    void openUndoPageRejectsLegacyV2FormatVersion() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction create = mgr.begin();
            SegmentRef seg = disk.createSegment(create, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(create, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(
                    UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), pid, pid);
            undoAccess.createFirstPage(create, pid, UndoLogKind.UPDATE, TransactionId.of(7), handle);
            mgr.commit(create);

            MiniTransaction corrupt = mgr.begin();
            int legacyV2Flags = (2 << UndoPageLayout.FORMAT_VERSION_SHIFT) | UndoPageLayout.FLAG_FIRST_PAGE;
            corrupt.getPage(pool, pid, PageLatchMode.EXCLUSIVE).writeBytes(
                    UndoPageLayout.PAGE_FLAGS, new byte[]{(byte) legacyV2Flags});
            mgr.commit(corrupt);

            MiniTransaction read = mgr.beginReadOnly();
            assertThrows(UndoLogFormatException.class,
                    () -> undoAccess.openUndoPage(read, pid, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(read);
        });
    }

    /** 未知的未来版本也不能按当前布局猜测读取。 */
    @Test
    void openUndoPageRejectsUnknownFormatVersion() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction create = mgr.begin();
            SegmentRef seg = disk.createSegment(create, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(create, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(
                    UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), pid, pid);
            undoAccess.createFirstPage(create, pid, UndoLogKind.INSERT, TransactionId.of(7), handle);
            mgr.commit(create);

            MiniTransaction corrupt = mgr.begin();
            int unknownFlags = ((UndoPageLayout.CURRENT_FORMAT_VERSION + 1)
                    << UndoPageLayout.FORMAT_VERSION_SHIFT) | UndoPageLayout.FLAG_FIRST_PAGE;
            corrupt.getPage(pool, pid, PageLatchMode.EXCLUSIVE).writeBytes(
                    UndoPageLayout.PAGE_FLAGS, new byte[]{(byte) unknownFlags});
            mgr.commit(corrupt);

            MiniTransaction read = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> undoAccess.openUndoPage(read, pid, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(read);
        });
    }

    /** 落盘 pair 损坏属于 undo 格式错误，不能向恢复层泄漏普通参数校验异常。 */
    @Test
    void logicalHeadWrapsCorruptPairAsFormatException() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction create = mgr.begin();
            SegmentRef seg = disk.createSegment(create, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(create, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(
                    UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), pid, pid);
            undoAccess.createFirstPage(create, pid, UndoLogKind.INSERT, TransactionId.of(7), handle);
            mgr.commit(create);

            MiniTransaction corrupt = mgr.begin();
            RollPointer nonNull = new RollPointer(false, pid.pageNo(), UndoPageLayout.RECORD_AREA_START);
            corrupt.getPage(pool, pid, PageLatchMode.EXCLUSIVE).writeBytes(
                    UndoPageLayout.LOGICAL_HEAD_ROLL_POINTER, nonNull.encode());
            mgr.commit(corrupt);

            MiniTransaction read = mgr.begin();
            UndoPage page = undoAccess.openUndoPage(read, pid, PageLatchMode.SHARED);
            assertThrows(UndoLogFormatException.class, page::logicalHead);
            mgr.rollbackUncommitted(read);
        });
    }

    /**
     * registry-aware UNDO access 与 INDEX access 一样必须复核运行时表空间状态。INACTIVE 时不能继续打开旧 undo 页，
     * 也不能把已分配页破坏性重初始化成 UNDO first 页。
     */
    @Test
    void registryAwareUndoAccessRejectsInactiveTablespaceBeforeTouchingPage() {
        PageStore store = new FileChannelPageStore();
        store.create(UNDO_SPACE, dir.resolve("inactive-undo.ibu"), PS, PageNo.of(64));
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 16)) {
            TablespaceRegistry tablespaces = registryFor(TablespaceState.ACTIVE);
            tablespaces.markInactive(UNDO_SPACE);
            UndoPageAccess undoAccess = new UndoPageAccess(pool, PS, tablespaces);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            PageId pageId = PageId.of(UNDO_SPACE, PageNo.of(5));
            UndoSegmentHandle handle = new UndoSegmentHandle(UNDO_SPACE, 0, SegmentId.of(1), pageId, pageId);

            assertThrows(TablespaceUnavailableException.class,
                    () -> undoAccess.createFirstPage(m, pageId, UndoLogKind.INSERT, TransactionId.of(7), handle));
            assertThrows(TablespaceUnavailableException.class,
                    () -> undoAccess.openUndoPage(m, pageId, PageLatchMode.SHARED));
            assertTrue(mgr.redoLogManager().bufferedRecords().isEmpty(), "inactive access must not touch undo page");
            mgr.rollbackUncommitted(m);
        }
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

    private TablespaceRegistry registryFor(TablespaceState state) {
        TablespaceMetadata metadata = new TablespaceMetadata(UNDO_SPACE, "space-" + UNDO_SPACE.value(),
                TablespaceType.UNDO, PS, state,
                List.of(DataFileDescriptor.single(dir.resolve("registry-undo.ibu"), PageNo.of(0), PageNo.of(64))),
                new SpaceFlags(TablespaceTypeFlags.encode(TablespaceType.UNDO)), PageNo.of(64), PageNo.of(0), 1L);
        return new CachingTablespaceRegistry(spaceId -> UNDO_SPACE.equals(spaceId)
                ? java.util.Optional.of(metadata) : java.util.Optional.empty());
    }
}
