package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** T1.3a UndoLog 端到端：append→RollPointer→readRecord、prevRollPointer 链、双 newPage redo 顺序、持久化重读。 */
class UndoLogStoreTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final UndoLog undoLog = new UndoLog(registry);

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(64, true), 1)), true);
    }
    private static IndexKeyDef keyDef() {
        return new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }
    private UndoRecord rec(long undoNo, long id, RollPointer prev) {
        return UndoRecord.insert(UndoNo.of(undoNo), TransactionId.of(7),
                1L, 9L, List.of(new ColumnValue.IntValue(id)), prev);
    }

    @Test void appendReturnsNonNullPointerAndReadsBack() {
        onPool((mgr, disk, access) -> {
            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            UndoRecord r = rec(1, 100, RollPointer.NULL);
            RollPointer rp = undoLog.append(page, r, keyDef(), schema());
            assertFalse(rp.isNull());
            assertTrue(rp.insert());
            assertEquals(page.pageId().pageNo(), rp.pageNo());
            assertEquals(r, undoLog.readRecord(page, rp, keyDef(), schema()));
            mgr.commit(m);
        });
    }

    @Test void prevRollPointerChainsTwoRecords() {
        onPool((mgr, disk, access) -> {
            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            RollPointer rp1 = undoLog.append(page, rec(1, 100, RollPointer.NULL), keyDef(), schema());
            RollPointer rp2 = undoLog.append(page, rec(2, 101, rp1), keyDef(), schema());
            UndoRecord back2 = undoLog.readRecord(page, rp2, keyDef(), schema());
            assertEquals(rp1, back2.prevRollPointer());
            assertEquals(rec(1, 100, RollPointer.NULL),
                    undoLog.readRecord(page, back2.prevRollPointer(), keyDef(), schema()));
            mgr.commit(m);
        });
    }

    @Test void readRecordRejectsPointerFromOtherPage() {
        onPool((mgr, disk, access) -> {
            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            RollPointer rp = undoLog.append(page, rec(1, 100, RollPointer.NULL), keyDef(), schema());
            RollPointer wrongPage = new RollPointer(true, PageNo.of(rp.pageNo().value() + 1), rp.offset());
            assertThrows(UndoLogFormatException.class,
                    () -> undoLog.readRecord(page, wrongPage, keyDef(), schema()));
            mgr.commit(m);
        });
    }

    @Test void doubleNewPageEndsAsUndoAndSurvivesReload() {
        onPool((mgr, disk, access) -> {
            // 同一 MTR：allocatePage(ALLOCATED) 后 createFirstPage(UNDO)，append 一条，commit。
            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            PageId pid = page.pageId();
            RollPointer rp = undoLog.append(page, rec(1, 100, RollPointer.NULL), keyDef(), schema());
            mgr.commit(m);

            // 新 MTR 重读：页类型仍为 UNDO（两条 PAGE_INIT 的 redo 顺序最终态），record 完好。
            MiniTransaction r = mgr.begin();
            UndoPage reopened = access.openUndoPage(r, pid, PageLatchMode.SHARED);
            assertEquals(UndoLogKind.INSERT, reopened.undoKind());
            assertEquals(1, reopened.recordCount());
            assertEquals(rec(1, 100, RollPointer.NULL),
                    undoLog.readRecord(reopened, rp, keyDef(), schema()));
            mgr.rollbackUncommitted(r);
        });
    }

    @Test void appendSurvivesStoreReopen() {
        Path path = dir.resolve("undo-reopen.ibu");
        UndoRecord expected = rec(1, 100, RollPointer.NULL);
        PageId pid;
        RollPointer rp;

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoPageAccess access = new UndoPageAccess(pool, PS);

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, path, PageNo.of(64));
            mgr.commit(boot);

            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            pid = page.pageId();
            rp = undoLog.append(page, expected, keyDef(), schema());
            mgr.commit(m);
        }

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(UNDO_SPACE, path, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            UndoPageAccess access = new UndoPageAccess(pool, PS);

            MiniTransaction r = mgr.begin();
            UndoPage reopened = access.openUndoPage(r, pid, PageLatchMode.SHARED);
            assertEquals(UndoLogKind.INSERT, reopened.undoKind());
            assertEquals(expected, undoLog.readRecord(reopened, rp, keyDef(), schema()));
            mgr.rollbackUncommitted(r);
        }
    }

    // ---- harness ----

    private interface PoolBody { void run(MiniTransactionManager mgr, DiskSpaceManager disk, UndoPageAccess access); }

    private UndoPage freshUndoPage(MiniTransaction m, DiskSpaceManager disk, UndoPageAccess access) {
        var seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
        PageId pid = disk.allocatePage(m, seg);
        UndoSegmentHandle handle = new UndoSegmentHandle(UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), pid, pid);
        return access.createFirstPage(m, pid, UndoLogKind.INSERT, TransactionId.of(7), handle);
    }

    private void onPool(PoolBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoPageAccess access = new UndoPageAccess(pool, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, disk, access);
        }
    }
}
