package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeDeleteMarkResult;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeInsertResult;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3d RollbackService：反向走 INSERT undo 链物理删除未提交聚簇行 + 回收内存 slot。整栈 test-wired
 * （assignWriteId → beforeInsert → insertClustered → rollback），无生产组合根。覆盖：单行/多行 full rollback、
 * orphan undo 幂等、只读/未写事务仅翻状态。
 *
 * <p><b>非目标</b>（T1.3e+）：UPDATE/DELETE undo、savepoint/statement rollback、恢复期 rollback、undo 页回收。
 */
class RollbackServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId DATA_SPACE = SpaceId.of(41);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void singleRowInsertRollbackRemovesRowAndReleasesSlot() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer rp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, rp);
            ctx.mgr.commit(m);
            assertEquals(1, ctx.slots.activeSlotCount(), "slot claimed on first write");

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(1, summary.undoRecordsApplied(), "one INSERT undo applied");
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertEquals(0, ctx.slots.activeSlotCount(), "slot released after rollback");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, search(1)).isEmpty(), "inserted row removed by rollback");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void multipleRowsRollbackReverseWalkRemovesAll() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            for (int i = 1; i <= 3; i++) {
                MiniTransaction m = ctx.mgr.begin();
                RollPointer rp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID,
                        key(i), index.keyDef(), index.schema());
                BTreeInsertResult res = svc.insertClustered(m, index, row(i), wid, rp);
                index = res.indexAfterInsert();
                ctx.mgr.commit(m);
            }

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(3, summary.undoRecordsApplied(), "reverse walk applies all three INSERT undo");
            assertEquals(0, ctx.slots.activeSlotCount());
            MiniTransaction r = ctx.mgr.begin();
            for (int i = 1; i <= 3; i++) {
                assertTrue(svc.lookup(r, index, search(i)).isEmpty(), "row " + i + " removed");
            }
            ctx.mgr.commit(r);
        });
    }

    @Test
    void orphanUndoRollbackIsIdempotent() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.txnMgr.assignWriteId(txn);
            // 只写 undo，不写聚簇行：模拟「失败插入」留下的 orphan undo
            MiniTransaction m = ctx.mgr.begin();
            ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            ctx.mgr.commit(m);
            assertEquals(1, ctx.slots.activeSlotCount());

            // rollback 走链：deleteClustered 找不到对应行 → no-op，不抛
            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(1, summary.undoRecordsApplied(), "orphan undo still consumed (idempotent no-op delete)");
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertEquals(0, ctx.slots.activeSlotCount(), "slot released even when no row existed");
        });
    }

    @Test
    void readOnlyOrUnwrittenTxnRollbackJustFlipsState() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            // 未写入：undoContext 为 null

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(0, summary.undoRecordsApplied(), "no undo to apply");
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertEquals(0, ctx.slots.activeSlotCount(), "no slot touched");
        });
    }

    @Test
    void insertThenUpdateRollbackRemovesRowReversingBoth() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, insRp);
            ctx.mgr.commit(m);
            updateRow(ctx, svc, index, txn, wid, 1, "v2");
            MiniTransaction chk = ctx.mgr.begin();
            assertEquals("v2", payloadOf(svc.lookup(chk, index, search(1)).orElseThrow()));
            ctx.mgr.commit(chk);

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(2, summary.undoRecordsApplied(), "update + insert undo both reversed");
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, search(1)).isEmpty(), "row gone: update restored then insert deleted");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void sameRowDoubleUpdateRollbackChainRestoresThenDeletes() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, insRp); // payload-1
            ctx.mgr.commit(m);
            updateRow(ctx, svc, index, txn, wid, 1, "v2");
            updateRow(ctx, svc, index, txn, wid, 1, "v3");
            MiniTransaction chk = ctx.mgr.begin();
            assertEquals("v3", payloadOf(svc.lookup(chk, index, search(1)).orElseThrow()));
            ctx.mgr.commit(chk);

            // 反向走链：撤销 v3 update(恢复 v2)→撤销 v2 update(恢复 payload-1)→撤销 insert(物理删)
            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(3, summary.undoRecordsApplied(), "two updates + insert reversed in order");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, search(1)).isEmpty(), "fully rolled back to non-existence");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void deleteMarkThenRollbackRestoresLiveRow() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            // T1 插入并提交（存活、已提交）
            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId w1 = ctx.txnMgr.assignWriteId(t1);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(t1, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), w1, insRp);
            ctx.mgr.commit(m);
            ctx.txnMgr.commit(t1);
            ctx.undoMgr.onCommit(t1);

            // T2 delete-mark
            Transaction t2 = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId w2 = ctx.txnMgr.assignWriteId(t2);
            deleteMarkRow(ctx, svc, index, t2, w2, 1);
            MiniTransaction chk = ctx.mgr.begin();
            assertTrue(svc.lookup(chk, index, search(1)).isEmpty(), "delete-marked row filtered from lookup");
            ctx.mgr.commit(chk);

            // rollback T2 → 取消标记，行复活
            RollbackSummary summary = ctx.rollbackService.rollback(t2, index);

            assertEquals(1, summary.undoRecordsApplied());
            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = svc.lookup(r, index, search(1)).orElseThrow();
            ctx.mgr.commit(r);
            assertEquals(w1, found.record().hiddenColumns().dbTrxId(), "un-mark 还原存活版本 + 旧隐藏列");
        });
    }

    @Test
    void insertThenDeleteMarkRollbackRemovesRow() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, insRp);
            ctx.mgr.commit(m);
            deleteMarkRow(ctx, svc, index, txn, wid, 1); // 同事务内 insert→delete-mark

            // rollback：先取消标记(复活)再删 insert(物理删) → 行彻底消失
            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(2, summary.undoRecordsApplied(), "delete-mark + insert undo 反向应用");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookupIncludingDeleted(r, index, search(1)).isEmpty(), "行被物理移除（含 delete-marked 视角也空）");
            ctx.mgr.commit(r);
        });
    }

    // ---- helpers ----

    /** 前向 DELETE-mark 编排（test-wired，§16.3）：读存活当前版本 → beforeDelete 写 DELETE_MARK undo → setClusteredDeleteMark 置删除位。 */
    private void deleteMarkRow(Ctx ctx, SplitCapableBTreeIndexService svc, BTreeIndex index, Transaction txn,
                              TransactionId wid, long id) {
        MiniTransaction read = ctx.mgr.begin();
        BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
        ctx.mgr.commit(read);
        HiddenColumns oldHidden = old.record().hiddenColumns();
        MiniTransaction m = ctx.mgr.begin();
        RollPointer delRp = ctx.undoMgr.beforeDelete(txn, m, TABLE_ID, INDEX_ID, key(id),
                old.record().columnValues(), oldHidden, index.keyDef(), index.schema());
        BTreeDeleteMarkResult res = svc.setClusteredDeleteMark(m, index, search(id), true,
                new HiddenColumns(wid, delRp), oldHidden.dbTrxId(), oldHidden.dbRollPtr());
        ctx.mgr.commit(m);
        assertTrue(res.changed(), "delete-mark applied");
    }

    /** 前向 UPDATE 编排（test-wired，§7.3）：读旧 image → beforeUpdate 写 UPDATE undo → replaceClustered 盖新值。 */
    private void updateRow(Ctx ctx, SplitCapableBTreeIndexService svc, BTreeIndex index, Transaction txn,
                           TransactionId wid, long id, String newPayload) {
        MiniTransaction read = ctx.mgr.begin();
        BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
        ctx.mgr.commit(read);
        HiddenColumns oldHidden = old.record().hiddenColumns();
        MiniTransaction m = ctx.mgr.begin();
        RollPointer newRp = ctx.undoMgr.beforeUpdate(txn, m, TABLE_ID, INDEX_ID, key(id),
                old.record().columnValues(), oldHidden, index.keyDef(), index.schema());
        svc.replaceClustered(m, index, search(id),
                new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(newPayload)),
                        false, RecordType.CONVENTIONAL, new HiddenColumns(wid, newRp)),
                oldHidden.dbTrxId(), oldHidden.dbRollPtr());
        ctx.mgr.commit(m);
    }

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

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            body.run(new Ctx(store, pool));
        }
    }

    private interface Body {
        void run(Ctx ctx);
    }

    private final class Ctx {
        final MiniTransactionManager mgr = new MiniTransactionManager();
        final DiskSpaceManager disk;
        final IndexPageAccess access;
        final DiskSpaceUndoAllocator undoAllocator;
        final UndoLogSegmentAccess undoAccess;
        final RollbackSegmentSlotManager slots;
        final UndoLogManager undoMgr;
        final TransactionManager txnMgr = new TransactionManager(new TransactionSystem());
        final RollbackService rollbackService;
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private Ctx(PageStore store, BufferPool pool) {
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
            this.undoAllocator = new DiskSpaceUndoAllocator(disk);
            this.undoAccess = new UndoLogSegmentAccess(pool, PS, undoAllocator, registry);
            this.slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            this.undoMgr = new UndoLogManager(undoAccess, slots, UNDO_SPACE, new HistoryList());
            this.rollbackService = new RollbackService(service(), undoAccess, slots, txnMgr, mgr);
        }

        private SplitCapableBTreeIndexService service() {
            return new SplitCapableBTreeIndexService(access, disk, registry);
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
        }

        private BTreeIndex clusteredIndex() {
            return new BTreeIndex(INDEX_ID, rootPageId, 0, idKey(), clusteredSchema(), true,
                    leafSegment, nonLeafSegment);
        }
    }
}
