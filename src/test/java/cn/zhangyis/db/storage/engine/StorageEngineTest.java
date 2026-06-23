package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.TransactionOptions;
import cn.zhangyis.db.storage.trx.UndoLogManager;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * E1 StorageEngine：生命周期（open/close/状态机）+ WAL durable 往返（写→close→重开读回）+ 重开续写
 * （redo 边界安装使 LSN 连续）+ checkpoint 使 redo durable。整栈经引擎访问器驱动，证明组合根是生产可用的。
 */
class StorageEngineTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId DATA_SPACE = SpaceId.of(10);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;

    @TempDir
    Path dir;

    private EngineConfig config() {
        return new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    // ---- 生命周期 ----

    @Test
    void openWiresServicesAndPublishesOpen() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        assertEquals(EngineState.OPEN, engine.state());
        assertNotNull(engine.transactionManager());
        assertNotNull(engine.miniTransactionManager());
        assertNotNull(engine.diskSpaceManager());
        assertNotNull(engine.btreeService());
        assertNotNull(engine.undoLogManager());
        assertNotNull(engine.mvccReader());
        assertNotNull(engine.rollbackService());
        assertNotNull(engine.indexPageAccess());
        engine.close();
        assertEquals(EngineState.CLOSED, engine.state());
        engine.close(); // 幂等
        assertEquals(EngineState.CLOSED, engine.state());
    }

    @Test
    void closedEngineRejectsAccess() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        engine.close();
        assertThrows(EngineStateException.class, engine::diskSpaceManager);
        assertThrows(EngineStateException.class, engine::checkpoint);
    }

    @Test
    void doubleOpenRejected() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            assertThrows(EngineStateException.class, engine::open);
        } finally {
            engine.close();
        }
    }

    // ---- durable 往返 + 重开续写 ----

    @Test
    void durableRoundTripAcrossRestart() {
        EngineConfig cfg = config();
        Path dataPath = dir.resolve("data.ibd");

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRow(e1, index, 1, "v1");
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.open();
        e2.diskSpaceManager().openTablespace(DATA_SPACE, dataPath);
        MiniTransaction r = e2.miniTransactionManager().begin();
        BTreeLookupResult found = e2.btreeService().lookup(r, index, search(1)).orElseThrow();
        e2.miniTransactionManager().commit(r);
        assertEquals("v1", payloadOf(found), "row persists across clean restart (read from flushed data file)");
        e2.close();
    }

    @Test
    void reopenInstallsRedoBoundaryAndAllowsContinuedWrites() {
        EngineConfig cfg = config();
        Path dataPath = dir.resolve("data.ibd");

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRow(e1, index, 1, "v1");
        long tailLsn = e1.miniTransactionManager().redoLogManager().currentLsn().value();
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.open();
        assertEquals(tailLsn, e2.miniTransactionManager().redoLogManager().currentLsn().value(),
                "reopen installs redo boundary = durable tail LSN (no restart-from-0 overlap)");
        e2.diskSpaceManager().openTablespace(DATA_SPACE, dataPath);
        insertRow(e2, index, 2, "v2"); // 重开后续写
        e2.close();

        StorageEngine e3 = new StorageEngine(cfg);
        e3.open();
        e3.diskSpaceManager().openTablespace(DATA_SPACE, dataPath);
        MiniTransaction r = e3.miniTransactionManager().begin();
        assertEquals("v1", payloadOf(e3.btreeService().lookup(r, index, search(1)).orElseThrow()));
        assertEquals("v2", payloadOf(e3.btreeService().lookup(r, index, search(2)).orElseThrow()));
        e3.miniTransactionManager().commit(r);
        e3.close();
    }

    @Test
    void checkpointMakesRedoDurable() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        BTreeIndex index = createClusteredIndex(engine, dir.resolve("data.ibd"));
        insertRow(engine, index, 1, "v1");
        RedoLogManager redo = engine.miniTransactionManager().redoLogManager();
        engine.checkpoint();
        assertEquals(redo.currentLsn(), redo.flushedToDiskLsn(),
                "checkpoint flushes redo durable (WAL prerequisite for any data page flush)");
        engine.close();
    }

    // ---- helpers ----

    private BTreeIndex createClusteredIndex(StorageEngine engine, Path dataPath) {
        DiskSpaceManager disk = engine.diskSpaceManager();
        MiniTransactionManager mtrMgr = engine.miniTransactionManager();
        MiniTransaction boot = mtrMgr.begin();
        disk.createTablespace(boot, DATA_SPACE, dataPath, PageNo.of(64));
        SegmentRef leaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_LEAF);
        SegmentRef nonLeaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_NON_LEAF);
        PageId root = disk.allocatePage(boot, leaf);
        engine.indexPageAccess().createIndexPage(boot, root, INDEX_ID, 0);
        mtrMgr.commit(boot);
        return new BTreeIndex(INDEX_ID, root, 0, idKey(), clusteredSchema(), true, leaf, nonLeaf);
    }

    private void insertRow(StorageEngine engine, BTreeIndex index, long id, String payload) {
        TransactionManager txnMgr = engine.transactionManager();
        MiniTransactionManager mtrMgr = engine.miniTransactionManager();
        UndoLogManager undoMgr = engine.undoLogManager();
        SplitCapableBTreeIndexService svc = engine.btreeService();
        Transaction txn = txnMgr.begin(TransactionOptions.defaults());
        txnMgr.assignWriteId(txn);
        MiniTransaction m = mtrMgr.begin();
        RollPointer rp = undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(id)), index.keyDef(), index.schema());
        svc.insertClustered(m, index, row(id, payload), txn.transactionId(), rp);
        mtrMgr.commit(m);
        txnMgr.commit(txn);
        undoMgr.onCommit(txn);
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

    private static SearchKey search(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String payload) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(payload)), false, RecordType.CONVENTIONAL);
    }
}
