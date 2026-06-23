package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P6 PurgeCoordinator 端到端：单线程聚簇 purge + undo 段回收。整栈 test-wired
 * （assignWriteId → beforeX → Xclustered → commit+onCommit → runBatch）。覆盖：无 live ReadView 物理移除 delete-marked +
 * 回收；live ReadView 挡住、release 后放行；UPDATE-only 只回收段不碰记录（#9）；insert-only 立即回收；
 * per-entry 原子（drop 失败保留 history head，#8）。
 */
class PurgeCoordinatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId DATA_SPACE = SpaceId.of(41);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void purgesCommittedDeleteWhenNoLiveReadView() {
        onPool(false, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.commit(t1);
            Transaction t2 = ctx.beginRw();
            ctx.deleteMark(index, t2, 1);
            ctx.commit(t2);
            assertEquals(1, ctx.history.committedSize(), "delete txn enqueued to history");

            PurgeSummary summary = ctx.purge.runBatch(10);

            assertEquals(1, summary.purgedLogs());
            assertEquals(1, summary.removedRecords(), "delete-marked row physically removed");
            assertEquals(1, summary.reclaimedInsertSegments(), "T1 insert undo segment reclaimed");
            assertEquals(0, ctx.history.committedSize(), "history drained");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(ctx.svc.lookupIncludingDeleted(r, index, search(1)).isEmpty(), "row gone after purge");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void liveReadViewBlocksPurgeThenReleaseAllows() {
        onPool(false, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.commit(t1);

            // 在 delete 提交前打开 RR 快照：该快照看不到后续 delete，purge 必须保留 delete undo
            Transaction reader = ctx.txnMgr.begin(new TransactionOptions(IsolationLevel.REPEATABLE_READ, true, true));
            ctx.txnMgr.readViewManager().openReadView(reader);

            Transaction t2 = ctx.beginRw();
            ctx.deleteMark(index, t2, 1);
            ctx.commit(t2);

            PurgeSummary blocked = ctx.purge.runBatch(10);
            assertEquals(0, blocked.purgedLogs(), "live older ReadView blocks delete purge");
            assertEquals(1, ctx.history.committedSize(), "delete log retained while reader live");
            MiniTransaction chk = ctx.mgr.begin();
            assertTrue(ctx.svc.lookupIncludingDeleted(chk, index, search(1)).isPresent(), "row still present (marked)");
            ctx.mgr.commit(chk);

            ctx.txnMgr.readViewManager().release(reader); // 注销快照 → 边界推进

            PurgeSummary after = ctx.purge.runBatch(10);
            assertEquals(1, after.purgedLogs());
            assertEquals(1, after.removedRecords());
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(ctx.svc.lookupIncludingDeleted(r, index, search(1)).isEmpty(), "purged after reader released");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void updateOnlyReclaimsSegmentWithoutTouchingRecord() {
        onPool(false, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.commit(t1);
            Transaction t2 = ctx.beginRw();
            ctx.update(index, t2, 1, "v2");
            ctx.commit(t2);

            PurgeSummary summary = ctx.purge.runBatch(10);

            assertEquals(1, summary.purgedLogs(), "update undo log reclaimed");
            assertEquals(0, summary.removedRecords(), "UPDATE-only purge does not remove any clustered record");
            assertEquals(0, ctx.history.committedSize());
            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = ctx.svc.lookup(r, index, search(1)).orElseThrow();
            ctx.mgr.commit(r);
            assertEquals("v2", payloadOf(found), "current version untouched by purge");
        });
    }

    @Test
    void insertOnlyUndoReclaimedImmediately() {
        onPool(false, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.commit(t1);
            assertEquals(1, ctx.history.insertReclaimSize(), "insert-only commit enqueues reclaim");
            assertEquals(0, ctx.history.committedSize());

            PurgeSummary summary = ctx.purge.runBatch(10);

            assertEquals(1, summary.reclaimedInsertSegments());
            assertEquals(0, summary.removedRecords());
            assertEquals(0, ctx.history.insertReclaimSize());
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(ctx.svc.lookup(r, index, search(1)).isPresent(), "committed inserted row stays (only undo reclaimed)");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void perEntryAtomicityKeepsHistoryHeadOnDropFailure() {
        onPool(true, ctx -> { // failing dropUndoSegment
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            // 单事务 insert+delete-mark → 提交为含 delete undo 的 committed log（无独立 insert-reclaim）
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.deleteMark(index, t1, 1);
            ctx.commit(t1);
            assertEquals(1, ctx.history.committedSize());

            // drop 抛错：异常传播、history head 不被 poll
            assertThrows(DatabaseRuntimeException.class, () -> ctx.purge.runBatch(10));
            assertEquals(1, ctx.history.committedSize(), "history head retained on hard failure (per-entry atomic)");
        });
    }

    // ---- helpers ----

    private static String payloadOf(BTreeLookupResult r) {
        return ((ColumnValue.StringValue) r.record().columnValues().get(1)).value();
    }

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(200, true), 1)), true);
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static List<ColumnValue> key(long id) {
        return List.of(new ColumnValue.IntValue(id));
    }

    private static SearchKey search(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("payload-" + id)), false, RecordType.CONVENTIONAL);
    }

    private void onPool(boolean failDrop, Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            body.run(new Ctx(store, pool, failDrop));
        }
    }

    private interface Body {
        void run(Ctx ctx);
    }

    /** 仅在 dropUndoSegment 抛错的分配器包装，用于 per-entry 原子测试；create/allocate 透传真实分配器。 */
    private static final class FailingDropAllocator implements UndoSpaceAllocator {
        private final UndoSpaceAllocator delegate;

        private FailingDropAllocator(UndoSpaceAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        public UndoSegmentHandle createUndoSegment(MiniTransaction mtr, SpaceId undoSpace) {
            return delegate.createUndoSegment(mtr, undoSpace);
        }

        @Override
        public PageId allocatePage(MiniTransaction mtr, SpaceId undoSpace, int inodeSlot, SegmentId segmentId) {
            return delegate.allocatePage(mtr, undoSpace, inodeSlot, segmentId);
        }

        @Override
        public void dropUndoSegment(MiniTransaction mtr, UndoSegmentHandle handle) {
            throw new DatabaseRuntimeException("injected dropUndoSegment failure");
        }
    }

    private final class Ctx {
        final MiniTransactionManager mgr = new MiniTransactionManager();
        final DiskSpaceManager disk;
        final IndexPageAccess access;
        final UndoSpaceAllocator undoAllocator;
        final UndoLogSegmentAccess undoAccess;
        final RollbackSegmentSlotManager slots;
        final HistoryList history = new HistoryList();
        final TransactionManager txnMgr = new TransactionManager(new TransactionSystem());
        final UndoLogManager undoMgr;
        final SplitCapableBTreeIndexService svc;
        PurgeCoordinator purge;
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private Ctx(PageStore store, BufferPool pool, boolean failDrop) {
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
            UndoSpaceAllocator real = new DiskSpaceUndoAllocator(disk);
            this.undoAllocator = failDrop ? new FailingDropAllocator(real) : real;
            this.undoAccess = new UndoLogSegmentAccess(pool, PS, undoAllocator, registry);
            this.slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            this.undoMgr = new UndoLogManager(undoAccess, slots, UNDO_SPACE, history);
            this.svc = new SplitCapableBTreeIndexService(access, disk, registry);
        }

        private void boot() {
            MiniTransaction b = mgr.begin();
            disk.createTablespace(b, DATA_SPACE, dir.resolve("data.ibd"), PageNo.of(64));
            leafSegment = disk.createSegment(b, DATA_SPACE, SegmentPurpose.INDEX_LEAF);
            nonLeafSegment = disk.createSegment(b, DATA_SPACE, SegmentPurpose.INDEX_NON_LEAF);
            rootPageId = disk.allocatePage(b, leafSegment);
            access.createIndexPage(b, rootPageId, INDEX_ID, 0);
            disk.createTablespace(b, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(b);
            this.purge = new PurgeCoordinator(mgr, txnMgr.system(), history, undoAccess, undoAllocator,
                    slots, svc, clusteredIndex());
        }

        private BTreeIndex clusteredIndex() {
            return new BTreeIndex(INDEX_ID, rootPageId, 0, idKey(), clusteredSchema(), true,
                    leafSegment, nonLeafSegment);
        }

        private Transaction beginRw() {
            Transaction txn = txnMgr.begin(TransactionOptions.defaults());
            txnMgr.assignWriteId(txn);
            return txn;
        }

        private void insert(BTreeIndex index, Transaction txn, long id) {
            MiniTransaction m = mgr.begin();
            RollPointer rp = undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(id), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(id), txn.transactionId(), rp);
            mgr.commit(m);
        }

        private void deleteMark(BTreeIndex index, Transaction txn, long id) {
            MiniTransaction read = mgr.begin();
            BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
            mgr.commit(read);
            HiddenColumns oldHidden = old.record().hiddenColumns();
            MiniTransaction m = mgr.begin();
            RollPointer delRp = undoMgr.beforeDelete(txn, m, TABLE_ID, INDEX_ID, key(id),
                    old.record().columnValues(), oldHidden, index.keyDef(), index.schema());
            svc.setClusteredDeleteMark(m, index, search(id), true,
                    new HiddenColumns(txn.transactionId(), delRp), oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            mgr.commit(m);
        }

        private void update(BTreeIndex index, Transaction txn, long id, String payload) {
            MiniTransaction read = mgr.begin();
            BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
            mgr.commit(read);
            HiddenColumns oldHidden = old.record().hiddenColumns();
            MiniTransaction m = mgr.begin();
            RollPointer newRp = undoMgr.beforeUpdate(txn, m, TABLE_ID, INDEX_ID, key(id),
                    old.record().columnValues(), oldHidden, index.keyDef(), index.schema());
            svc.replaceClustered(m, index, search(id),
                    new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(payload)),
                            false, RecordType.CONVENTIONAL, new HiddenColumns(txn.transactionId(), newRp)),
                    oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            mgr.commit(m);
        }

        /** commit 编排：先 txnMgr.commit 分配 TransactionNo，再 onCommit 入 history（评审 #3 顺序）。 */
        private void commit(Transaction txn) {
            txnMgr.commit(txn);
            undoMgr.onCommit(txn);
        }
    }
}
