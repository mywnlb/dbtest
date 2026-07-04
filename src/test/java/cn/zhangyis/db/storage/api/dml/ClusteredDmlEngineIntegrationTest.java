package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.EngineTablespaceConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
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
import cn.zhangyis.db.storage.redo.DurabilityPolicy;
import cn.zhangyis.db.storage.trx.TransactionOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * StorageEngine 组合根对 DML facade 的生产接线测试。该类只从 public engine API 进入，
 * 用来防止 DML facade 只停留在单元测试 fixture 中而没有被真实生命周期构造。
 */
class ClusteredDmlEngineIntegrationTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId DATA_SPACE = SpaceId.of(10);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;

    @Test
    @DisplayName("StorageEngine exposes clustered DML facade after open")
    void engineExposesDmlFacadeAfterOpen(@TempDir Path dir) {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            assertNotNull(engine.dmlService());
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("Committed clustered DML facade writes survive engine reopen")
    void committedDmlFacadeWritesSurviveReopen(@TempDir Path dir) {
        Path dataPath = dir.resolve("dml-reopen.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dir, dataPath);

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertAndCommit(e1, index, 1, "v1");
        updateAndCommit(e1, index, 1, "v2");
        e1.checkpoint();
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.configureClusteredIndex(index);
        e2.open();
        try {
            assertEquals("v2", payloadOf(lookup(e2, index, 1).orElseThrow()),
                    "reopen must recover the committed DML facade state from redo/data pages");
        } finally {
            e2.close();
        }
    }

    private static EngineConfig config(Path dir) {
        return new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    private static EngineConfig configWithRecoveryTablespace(Path dir, Path dataPath) {
        return new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024,
                List.of(new EngineTablespaceConfig(DATA_SPACE, dataPath)));
    }

    private static BTreeIndex createClusteredIndex(StorageEngine engine, Path dataPath) {
        DiskSpaceManager disk = engine.diskSpaceManager();
        MiniTransaction boot = engine.miniTransactionManager().begin();
        disk.createTablespace(boot, DATA_SPACE, dataPath, PageNo.of(64));
        SegmentRef leaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_LEAF);
        SegmentRef nonLeaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_NON_LEAF);
        PageId root = disk.allocatePage(boot, leaf);
        engine.indexPageAccess().createIndexPage(boot, root, INDEX_ID, 0);
        engine.miniTransactionManager().commit(boot);
        return new BTreeIndex(INDEX_ID, root, 0, idKey(), clusteredSchema(), true, leaf, nonLeaf);
    }

    private static void insertAndCommit(StorageEngine engine, BTreeIndex index, long id, String payload) {
        var txn = engine.transactionManager().begin(TransactionOptions.defaults());
        engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(id), row(id, payload),
                TABLE_ID, Duration.ofSeconds(1)));
        engine.dmlService().commit(new DmlCommitCommand(txn, DurabilityPolicy.FLUSH_ON_COMMIT,
                Duration.ofSeconds(2)));
    }

    private static void updateAndCommit(StorageEngine engine, BTreeIndex index, long id, String payload) {
        var txn = engine.transactionManager().begin(TransactionOptions.defaults());
        engine.dmlService().update(new ClusteredUpdateCommand(txn, index, search(id), row(id, payload),
                TABLE_ID, Duration.ofSeconds(1)));
        engine.dmlService().commit(new DmlCommitCommand(txn, DurabilityPolicy.FLUSH_ON_COMMIT,
                Duration.ofSeconds(2)));
    }

    private static Optional<BTreeLookupResult> lookup(StorageEngine engine, BTreeIndex index, long id) {
        MiniTransaction read = engine.miniTransactionManager().begin();
        try {
            Optional<BTreeLookupResult> found = engine.btreeService().lookup(read, index, search(id));
            engine.miniTransactionManager().commit(read);
            return found;
        } catch (RuntimeException e) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw e;
        }
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(200, true), 1)), true);
    }

    private static SearchKey search(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String payload) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(payload)), false, RecordType.CONVENTIONAL);
    }

    private static String payloadOf(BTreeLookupResult result) {
        return ((ColumnValue.StringValue) result.record().columnValues().get(1)).value();
    }
}
