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
import cn.zhangyis.db.storage.api.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeInsertResult;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3c undo 写路径全栈接线：显式执行 {@code assignWriteId → UndoLogManager.beforeInsert →
 * SplitCapableBTreeIndexService.insertClustered}，断言聚簇记录隐藏列 {@code DB_TRX_ID} 为事务写 id、
 * {@code DB_ROLL_PTR} 为 {@code beforeInsert} 返回值。整栈仍 test-wired、无生产组合根，orchestration 由本测试
 * 驱动（不新建 api facade，见 spec 关键决策④）。
 *
 * <p><b>orphan undo 风险（已知缺口，留 T1.3d+）</b>：本片只在成功插入路径接线。MTR rollback 不做 content undo，
 * 若 {@code beforeInsert} 已追加 undo record 后聚簇写失败并 MTR rollback，会留下指向无对应聚簇行的 orphan undo；
 * 失败插入的原子清理（rollback 反向走链 / slot 回收）留 DML facade / rollback 片，并在 current map 标为缺口。
 * 本测试名显式记录该风险，不覆盖失败路径。
 */
class UndoWritePathWiringTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId DATA_SPACE = SpaceId.of(41);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void successfulPathStampsRealRollPtrAndTrxId_orphanUndoOnFailureLeftToT13d() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);

            // undo 写路径 + 聚簇写同 MTR（WAL：同 redo batch）
            MiniTransaction m = ctx.mgr.begin();
            RollPointer rp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID,
                    clusterKey(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, rp);
            ctx.mgr.commit(m);

            // 读回验证隐藏列
            MiniTransaction read = ctx.mgr.begin();
            BTreeLookupResult found = svc.lookup(read, index, searchKey(1)).orElseThrow();
            ctx.mgr.commit(read);

            assertEquals(wid, found.record().hiddenColumns().dbTrxId(),
                    "DB_TRX_ID must be the transaction's assigned write id");
            assertEquals(rp, found.record().hiddenColumns().dbRollPtr(),
                    "DB_ROLL_PTR must be the roll pointer returned by beforeInsert (not NULL)");
            assertFalse(rp.isNull());
            assertTrue(rp.insert(), "insert undo roll pointer");
            ctx.txnMgr.commit(txn);
        });
    }

    @Test
    void multipleRowsEachGetDistinctRollPtrInSameTransaction() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);

            RollPointer[] rps = new RollPointer[3];
            for (int i = 0; i < 3; i++) {
                MiniTransaction m = ctx.mgr.begin();
                rps[i] = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID,
                        clusterKey(10 + i), index.keyDef(), index.schema());
                BTreeInsertResult res = svc.insertClustered(m, index, row(10 + i), wid, rps[i]);
                index = res.indexAfterInsert();
                ctx.mgr.commit(m);
            }

            // 每行的 DB_ROLL_PTR 各等于该行 beforeInsert 返回值；undoNo 在事务内递增
            for (int i = 0; i < 3; i++) {
                MiniTransaction read = ctx.mgr.begin();
                BTreeLookupResult found = svc.lookup(read, index, searchKey(10 + i)).orElseThrow();
                ctx.mgr.commit(read);
                assertEquals(wid, found.record().hiddenColumns().dbTrxId());
                assertEquals(rps[i], found.record().hiddenColumns().dbRollPtr(),
                        "row " + (10 + i) + " must carry its own undo roll pointer");
            }
            assertEquals(3, txn.undoContext().lastUndoNo().value(), "undoNo increments per row in same txn");
            ctx.txnMgr.commit(txn);
        });
    }

    // ---- helpers ----

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(200, true), 1)), true);
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static List<ColumnValue> clusterKey(long id) {
        return List.of(new ColumnValue.IntValue(id));
    }

    private static SearchKey searchKey(long id) {
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
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private Ctx(PageStore store, BufferPool pool) {
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
            this.undoAllocator = new DiskSpaceUndoAllocator(disk);
            this.undoAccess = new UndoLogSegmentAccess(pool, PS, undoAllocator, registry);
            this.slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            this.undoMgr = new UndoLogManager(undoAccess, slots, UNDO_SPACE);
        }

        private SplitCapableBTreeIndexService service() {
            return new SplitCapableBTreeIndexService(access, disk, registry);
        }

        /** 建数据表空间 + 聚簇索引 root，建 undo 表空间。 */
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
