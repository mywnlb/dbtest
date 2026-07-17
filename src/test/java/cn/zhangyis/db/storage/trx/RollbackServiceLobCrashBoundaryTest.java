package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.dml.ClusteredInsertCommand;
import cn.zhangyis.db.storage.api.ddl.BTreeIndexMetadataFactory;
import cn.zhangyis.db.storage.api.ddl.StorageColumnDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageColumnType;
import cn.zhangyis.db.storage.api.ddl.StorageColumnTypeId;
import cn.zhangyis.db.storage.api.ddl.StorageIndexDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageIndexKeyPart;
import cn.zhangyis.db.storage.api.ddl.StorageIndexOrder;
import cn.zhangyis.db.storage.api.ddl.StorageTableDefinition;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.api.dml.TableInsertCommand;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.engine.EngineConfig;
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
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** INSERT ownership 回滚在两个 durable crash 边界上的 LOB 可达性、logical-head 与幂等重试测试。 */
class RollbackServiceLobCrashBoundaryTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(10);
    private static final long TABLE_ID = 1L;
    private static final long INDEX_ID = 9L;

    @TempDir
    Path directory;

    /** inverse 已提交但 marker 未提交时 LOB 仍归 undo ownership；重试 inverse no-op 后才随 marker 原子释放。 */
    @Test
    void retryAfterInverseCommitReclaimsLobWithProgressMarker() {
        try (Fixture fixture = openFixture("after-inverse.ibd")) {
            Inserted inserted = fixture.insertExternal(1);
            UndoLogicalHead originalHead = UndoTestContexts.head(inserted.transaction().undoContext());
            AtomicBoolean fired = new AtomicBoolean();
            RollbackService crashable = fixture.engine().rollbackService()
                    .withProgressFaultInjectorForTest((phase, head) -> {
                        if (phase == RollbackProgressPhase.AFTER_INVERSE_COMMIT
                                && fired.compareAndSet(false, true)) {
                            throw new DatabaseRuntimeException("simulated crash after inverse commit");
                        }
                    });

            assertThrows(DatabaseRuntimeException.class, () -> crashable.rollback(inserted.transaction()));
            assertEquals(TransactionState.ROLLING_BACK, inserted.transaction().state());
            assertTrue(fixture.lookup(1).isEmpty(), "inverse commit must already remove the clustered row");
            assertEquals(originalHead, UndoTestContexts.head(inserted.transaction().undoContext()),
                    "logical head must not move before marker commit");
            assertTrue(fixture.lobReadable(inserted.external()),
                    "LOB ownership remains reachable until marker commits");

            assertEquals(1, crashable.rollback(inserted.transaction()).undoRecordsApplied(),
                    "retry replays the idempotent inverse before advancing the stale head");
            assertEquals(TransactionState.ROLLED_BACK, inserted.transaction().state());
            assertFalse(fixture.lobReadable(inserted.external()),
                    "marker retry must reclaim the INSERT-owned LOB pages");
            fixture.engine().lockManager().releaseAll(inserted.transaction().transactionId());
        }
    }

    /** marker 已提交后 row 与 LOB 均不可达、内存 head 也为 EMPTY；重试不得重复 free，只做事务终结。 */
    @Test
    void retryAfterProgressCommitOnlyFinalizesTransaction() {
        try (Fixture fixture = openFixture("after-progress.ibd")) {
            Inserted inserted = fixture.insertExternal(2);
            AtomicBoolean fired = new AtomicBoolean();
            RollbackService crashable = fixture.engine().rollbackService()
                    .withProgressFaultInjectorForTest((phase, head) -> {
                        if (phase == RollbackProgressPhase.AFTER_PROGRESS_COMMIT
                                && head.isEmpty() && fired.compareAndSet(false, true)) {
                            throw new DatabaseRuntimeException("simulated crash after progress commit");
                        }
                    });

            assertThrows(DatabaseRuntimeException.class, () -> crashable.rollback(inserted.transaction()));
            assertEquals(TransactionState.ROLLING_BACK, inserted.transaction().state());
            assertTrue(fixture.lookup(2).isEmpty());
            assertTrue(UndoTestContexts.head(inserted.transaction().undoContext()).isEmpty());
            assertFalse(fixture.lobReadable(inserted.external()),
                    "LOB free and logical-head movement share the committed marker MTR");

            assertEquals(0, crashable.rollback(inserted.transaction()).undoRecordsApplied(),
                    "EMPTY durable head means retry must not apply/free the record again");
            assertEquals(TransactionState.ROLLED_BACK, inserted.transaction().state());
            fixture.engine().lockManager().releaseAll(inserted.transaction().transactionId());
        }
    }

    /**
     * 第一棵 secondary inverse 提交后中断时，聚簇行仍作为当前完整行 image；重试必须接受第一棵 entry 已 ABSENT，
     * 继续撤销第二棵 secondary，最后删除聚簇行并推进 marker。
     */
    @Test
    void retryAfterOneSecondaryInverseCommitConvergesAllIndexes() {
        StorageEngine engine = new StorageEngine(new EngineConfig(directory, PAGE_SIZE, 256,
                SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(10), 64L * 1024 * 1024));
        MutableTargetResolver resolver = new MutableTargetResolver();
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createSecondaryTable(engine, directory.resolve("secondary-crash.ibd"));
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            LogicalRecord row = secondaryRow(1, "secondary-crash@example.test", 7);
            Transaction transaction = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.tableDmlService().insert(new TableInsertCommand(transaction, table, row,
                    Optional.empty(), Duration.ofSeconds(1)));

            AtomicInteger secondaryCommits = new AtomicInteger();
            RollbackService crashable = engine.rollbackService().withProgressFaultInjectorForTest((phase, head) -> {
                if (phase == RollbackProgressPhase.AFTER_SECONDARY_INVERSE_COMMIT
                        && secondaryCommits.incrementAndGet() == 1) {
                    throw new DatabaseRuntimeException("simulated crash after first secondary inverse");
                }
            });

            assertThrows(DatabaseRuntimeException.class, () -> crashable.rollback(transaction));
            assertEquals(TransactionState.ROLLING_BACK, transaction.state());
            assertTrue(clusteredIncludingDeleted(engine, table, 1).isPresent(),
                    "cluster inverse must remain last");
            assertTrue(secondaryIncludingDeleted(engine, table.secondaryIndexes().get(0), row).isEmpty());
            assertTrue(secondaryIncludingDeleted(engine, table.secondaryIndexes().get(1), row).isPresent());

            assertEquals(1, crashable.rollback(transaction).undoRecordsApplied());
            assertEquals(TransactionState.ROLLED_BACK, transaction.state());
            assertTrue(clusteredIncludingDeleted(engine, table, 1).isEmpty());
            assertTrue(secondaryIncludingDeleted(engine, table.secondaryIndexes().get(0), row).isEmpty());
            assertTrue(secondaryIncludingDeleted(engine, table.secondaryIndexes().get(1), row).isEmpty());
            engine.lockManager().releaseAll(transaction.transactionId());
        } finally {
            engine.close();
        }
    }

    /** 创建一棵聚簇树与两棵二级树，供跨树 rollback crash 边界测试使用。 */
    private static TableIndexMetadata createSecondaryTable(StorageEngine engine, Path dataPath) {
        StorageTableDefinition definition = new StorageTableDefinition(TABLE_ID, SPACE, dataPath,
                1, PageNo.of(128),
                List.of(new StorageColumnDefinition(1, "id", 0,
                                StorageColumnType.bigint(false, false)),
                        new StorageColumnDefinition(2, "email", 1,
                                new StorageColumnType(StorageColumnTypeId.VARCHAR, true,
                                        160, 0, false, 1, 2, List.of())),
                        new StorageColumnDefinition(3, "tenant_id", 2,
                                StorageColumnType.bigint(false, false))),
                List.of(new StorageIndexDefinition(201, "PRIMARY", true, true,
                                List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))),
                        new StorageIndexDefinition(202, "uq_email", true, false,
                                List.of(new StorageIndexKeyPart(2, StorageIndexOrder.ASC, 0))),
                        new StorageIndexDefinition(203, "idx_tenant", false, false,
                                List.of(new StorageIndexKeyPart(3, StorageIndexOrder.ASC, 0)))));
        TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition);
        return new BTreeIndexMetadataFactory().createTable(definition, binding);
    }

    /** 构造 secondary crash 测试使用的完整聚簇行。 */
    private static LogicalRecord secondaryRow(long id, String email, long tenantId) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(email), new ColumnValue.IntValue(tenantId)),
                false, RecordType.CONVENTIONAL);
    }

    /** 读取包含 delete-marked 状态的聚簇物理记录，并在返回前释放短读 MTR。 */
    private static Optional<BTreeLookupResult> clusteredIncludingDeleted(StorageEngine engine,
                                                                          TableIndexMetadata table,
                                                                          long id) {
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            Optional<BTreeLookupResult> result = engine.btreeService().lookupIncludingDeleted(
                    read, table.clusteredIndex(), key(id));
            engine.miniTransactionManager().commit(read);
            return result;
        } catch (RuntimeException failure) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw failure;
        }
    }

    /** 精确读取一条 secondary physical identity，保留其 delete 位。 */
    private static Optional<BTreeLookupResult> secondaryIncludingDeleted(StorageEngine engine,
                                                                          SecondaryIndexMetadata secondary,
                                                                          LogicalRecord row) {
        LogicalRecord entry = secondary.layout().toEntry(row, false);
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            Optional<BTreeLookupResult> result = engine.btreeService().lookupIncludingDeleted(
                    read, secondary.index(), secondary.layout().physicalKey(entry));
            engine.miniTransactionManager().commit(read);
            return result;
        } catch (RuntimeException failure) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw failure;
        }
    }

    private Fixture openFixture(String fileName) {
        StorageEngine engine = new StorageEngine(new EngineConfig(directory, PAGE_SIZE, 256,
                SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(10), 64L * 1024 * 1024));
        MutableTargetResolver resolver = new MutableTargetResolver();
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        LobIndexSetup setup = createLobClusteredIndex(engine, directory.resolve(fileName));
        resolver.install(new UndoTargetMetadata(new TableIndexMetadata(TABLE_ID,
                setup.index().schema().schemaVersion(), setup.index(), List.of()),
                Optional.of(setup.lobSegment())));
        return new Fixture(engine, setup);
    }

    /** 创建 leaf、non-leaf 与 LOB segment，并在同一 boot MTR 发布可写聚簇 root。 */
    private static LobIndexSetup createLobClusteredIndex(StorageEngine engine, Path dataPath) {
        DiskSpaceManager disk = engine.diskSpaceManager();
        MiniTransaction boot = engine.miniTransactionManager().begin(
                engine.miniTransactionManager().budgetFor(RedoBudgetPurpose.ENGINE_BOOT));
        disk.createTablespace(boot, SPACE, dataPath, PageNo.of(64));
        SegmentRef leaf = disk.createSegment(boot, SPACE, SegmentPurpose.INDEX_LEAF);
        SegmentRef nonLeaf = disk.createSegment(boot, SPACE, SegmentPurpose.INDEX_NON_LEAF);
        SegmentRef lob = disk.createSegment(boot, SPACE, SegmentPurpose.LOB);
        PageId root = disk.allocatePage(boot, leaf);
        engine.indexPageAccess().createIndexPage(boot, root, INDEX_ID, 0);
        engine.miniTransactionManager().commit(boot);
        return new LobIndexSetup(new BTreeIndex(INDEX_ID, root, 0, keyDefinition(), schema(), true,
                leaf, nonLeaf), lob);
    }

    private static IndexKeyDef keyDefinition() {
        return new IndexKeyDef(INDEX_ID,
                List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "body", ColumnType.text(false), 1)), true);
    }

    private static SearchKey key(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private record LobIndexSetup(BTreeIndex index, SegmentRef lobSegment) {
    }

    private record Inserted(Transaction transaction, ColumnValue.ExternalValue external) {
    }

    /** 持有一个打开的真实 StorageEngine；read helper 总在 finally 关闭只读 MTR，避免故障断言泄漏 fix/latch。 */
    private record Fixture(StorageEngine engine, LobIndexSetup setup) implements AutoCloseable {

        private Inserted insertExternal(long id) {
            Transaction transaction = engine.transactionManager().begin(TransactionOptions.defaults());
            LogicalRecord row = new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                    new ColumnValue.StringValue("回滚故障".repeat(100))), false, RecordType.CONVENTIONAL);
            engine.dmlService().insert(new ClusteredInsertCommand(transaction, setup.index(), key(id), row,
                    TABLE_ID, Optional.of(setup.lobSegment()), Duration.ofSeconds(1)));
            ColumnValue.ExternalValue external = (ColumnValue.ExternalValue) lookup(id).orElseThrow()
                    .record().columnValues().get(1);
            return new Inserted(transaction, external);
        }

        private Optional<BTreeLookupResult> lookup(long id) {
            MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
            try {
                Optional<BTreeLookupResult> found = engine.btreeService().lookup(read, setup.index(), key(id));
                engine.miniTransactionManager().commit(read);
                return found;
            } catch (RuntimeException failure) {
                engine.miniTransactionManager().rollbackUncommitted(read);
                throw failure;
            }
        }

        private boolean lobReadable(ColumnValue.ExternalValue external) {
            MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
            try {
                engine.lobStorage().read(read, schema().column(1).type(), external);
                engine.miniTransactionManager().commit(read);
                return true;
            } catch (DatabaseRuntimeException expectedFreedOrCorruptReference) {
                engine.miniTransactionManager().rollbackUncommitted(read);
                return false;
            }
        }

        @Override
        public void close() {
            engine.close();
        }
    }

    /** open 前安装到 StorageEngine，建物理索引后再发布权威 index+LOB binding。 */
    private static final class MutableTargetResolver
            implements IndexMetadataResolver, UndoTargetMetadataResolver {
        private UndoTargetMetadata target;

        private void install(UndoTargetMetadata target) {
            this.target = target;
        }

        @Override
        public BTreeIndex resolve(long tableId, long indexId) {
            return requireTarget(tableId, indexId).clusteredIndex();
        }

        @Override
        public UndoTargetMetadata resolveTarget(long tableId, long indexId) {
            return requireTarget(tableId, indexId);
        }

        private UndoTargetMetadata requireTarget(long tableId, long indexId) {
            if (target == null || tableId != TABLE_ID || target.clusteredIndex().indexId() != indexId) {
                throw new DatabaseValidationException("unknown test undo target");
            }
            return target;
        }
    }
}
