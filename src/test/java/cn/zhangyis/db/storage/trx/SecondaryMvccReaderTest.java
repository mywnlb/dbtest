package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.BTreeIndexMetadataFactory;
import cn.zhangyis.db.storage.api.ddl.StorageColumnDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageColumnType;
import cn.zhangyis.db.storage.api.ddl.StorageColumnTypeId;
import cn.zhangyis.db.storage.api.ddl.StorageIndexDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageIndexKeyPart;
import cn.zhangyis.db.storage.api.ddl.StorageIndexOrder;
import cn.zhangyis.db.storage.api.ddl.StorageTableDefinition;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.api.dml.DmlCommitCommand;
import cn.zhangyis.db.storage.api.dml.ResolvedDmlRollbackCommand;
import cn.zhangyis.db.storage.api.dml.TableInsertCommand;
import cn.zhangyis.db.storage.api.dml.TableUpdateCommand;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.redo.DurabilityPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 二级候选回表后按聚簇版本链复核可见性与逻辑 key 的生产组合测试。 */
class SecondaryMvccReaderTest {

    private static final long TABLE_ID = 61;
    private static final long PRIMARY_ID = 601;
    private static final long EMAIL_ID = 602;
    private static final SpaceId DATA_SPACE = SpaceId.of(2061);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @TempDir
    Path directory;

    /**
     * UPDATE 已发布 new-live/old-marked secondary，但对更新前 ReadView 不可见时，回表必须重建旧 A 行并复核谓词：
     * A 候选命中，B 候选因可见行重算为 A 而被过滤；提交后旧 RR view 仍保持相同结果，新 view 才看见 B。
     */
    @Test
    @DisplayName("secondary MVCC rechecks visible clustered version after key-changing update")
    void rechecksVisibleVersionAfterUncommittedAndCommittedUpdate() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine);
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            SecondaryIndexMetadata email = table.requireSecondary(EMAIL_ID);
            LogicalRecord before = row(1, "Alice@example.test");
            LogicalRecord after = row(1, "Bob@example.test");

            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(insert, table, before,
                    Optional.empty(), TIMEOUT));
            commit(engine, insert);

            Transaction oldReader = transaction(engine);
            ReadView oldView = engine.transactionManager().readViewManager().openReadView(oldReader);
            Transaction update = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(update, table, primaryKey(1), after, TIMEOUT));

            assertEquals(before.columnValues(), engine.secondaryMvccReader()
                    .readUnique(oldView, table, email, emailKey("ALICE@example.test"))
                    .orElseThrow().columnValues());
            assertTrue(engine.secondaryMvccReader()
                    .readUnique(oldView, table, email, emailKey("Bob@example.test")).isEmpty());

            commit(engine, update);
            assertEquals(before.columnValues(), engine.secondaryMvccReader()
                    .readUnique(oldView, table, email, emailKey("alice@example.test"))
                    .orElseThrow().columnValues());
            assertTrue(engine.secondaryMvccReader()
                    .readUnique(oldView, table, email, emailKey("bob@example.test")).isEmpty());

            Transaction newReader = transaction(engine);
            ReadView newView = engine.transactionManager().readViewManager().openReadView(newReader);
            assertEquals(after.columnValues(), engine.secondaryMvccReader()
                    .readUnique(newView, table, email, emailKey("BOB@example.test"))
                    .orElseThrow().columnValues());
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(oldReader));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(newReader));
        } finally {
            engine.close();
        }
    }

    /** 未提交 INSERT 对其它 ReadView 不可见但对 writer 自己可见；可见 DELETE 前的旧快照仍能通过 marked entry 回表。 */
    @Test
    @DisplayName("secondary MVCC handles uncommitted insert and delete")
    void handlesUncommittedInsertAndDelete() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "secondary-insert-delete.ibd", true);
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            SecondaryIndexMetadata email = table.requireSecondary(EMAIL_ID);
            LogicalRecord row = row(1, "visibility@example.test");

            Transaction writer = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(writer, table, row,
                    Optional.empty(), TIMEOUT));
            Transaction otherReader = transaction(engine);
            ReadView otherView = engine.transactionManager().readViewManager().openReadView(otherReader);
            ReadView ownView = engine.transactionManager().readViewManager().openReadView(writer);
            assertTrue(engine.secondaryMvccReader()
                    .readUnique(otherView, table, email, emailKey("visibility@example.test")).isEmpty());
            assertEquals(row.columnValues(), engine.secondaryMvccReader()
                    .readUnique(ownView, table, email, emailKey("VISIBILITY@example.test"))
                    .orElseThrow().columnValues());
            commit(engine, writer);

            Transaction beforeDeleteReader = transaction(engine);
            ReadView beforeDelete = engine.transactionManager().readViewManager()
                    .openReadView(beforeDeleteReader);
            Transaction delete = transaction(engine);
            engine.tableDmlService().delete(new cn.zhangyis.db.storage.api.dml.TableDeleteCommand(
                    delete, table, primaryKey(1), TIMEOUT));
            assertEquals(row.columnValues(), engine.secondaryMvccReader()
                    .readUnique(beforeDelete, table, email, emailKey("visibility@example.test"))
                    .orElseThrow().columnValues());
            commit(engine, delete);
            assertEquals(row.columnValues(), engine.secondaryMvccReader()
                    .readUnique(beforeDelete, table, email, emailKey("visibility@example.test"))
                    .orElseThrow().columnValues());

            Transaction afterDeleteReader = transaction(engine);
            ReadView afterDelete = engine.transactionManager().readViewManager().openReadView(afterDeleteReader);
            assertTrue(engine.secondaryMvccReader()
                    .readUnique(afterDelete, table, email, emailKey("visibility@example.test")).isEmpty());
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(otherReader));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(beforeDeleteReader));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(afterDeleteReader));
        } finally {
            engine.close();
        }
    }

    /** A→B→A 后，旧、中、新三个 ReadView 必须分别通过当前 live/历史 marked 候选回表得到各自版本。 */
    @Test
    @DisplayName("secondary MVCC preserves A to B to A snapshots")
    void preservesAtoBtoASnapshots() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "secondary-aba.ibd", true);
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            SecondaryIndexMetadata email = table.requireSecondary(EMAIL_ID);
            LogicalRecord versionA = row(1, "a@example.test");
            LogicalRecord versionB = row(1, "b@example.test");

            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(insert, table, versionA,
                    Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction oldReader = transaction(engine);
            ReadView oldView = engine.transactionManager().readViewManager().openReadView(oldReader);

            Transaction toB = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(toB, table, primaryKey(1), versionB, TIMEOUT));
            commit(engine, toB);
            Transaction middleReader = transaction(engine);
            ReadView middleView = engine.transactionManager().readViewManager().openReadView(middleReader);

            Transaction backToA = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(
                    backToA, table, primaryKey(1), versionA, TIMEOUT));
            commit(engine, backToA);
            Transaction currentReader = transaction(engine);
            ReadView currentView = engine.transactionManager().readViewManager().openReadView(currentReader);

            assertEquals(versionA.columnValues(), engine.secondaryMvccReader()
                    .readUnique(oldView, table, email, emailKey("a@example.test"))
                    .orElseThrow().columnValues());
            assertTrue(engine.secondaryMvccReader()
                    .readUnique(oldView, table, email, emailKey("b@example.test")).isEmpty());
            assertEquals(versionB.columnValues(), engine.secondaryMvccReader()
                    .readUnique(middleView, table, email, emailKey("b@example.test"))
                    .orElseThrow().columnValues());
            assertTrue(engine.secondaryMvccReader()
                    .readUnique(middleView, table, email, emailKey("a@example.test")).isEmpty());
            assertEquals(versionA.columnValues(), engine.secondaryMvccReader()
                    .readUnique(currentView, table, email, emailKey("A@example.test"))
                    .orElseThrow().columnValues());

            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(oldReader));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(middleReader));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(currentReader));
        } finally {
            engine.close();
        }
    }

    /** logical unique metadata 若解析出两个不同可见主键，reader 必须报告损坏而不是按物理顺序任选第一行。 */
    @Test
    @DisplayName("secondary unique MVCC fails closed on multiple visible rows")
    void failsClosedOnMultipleVisibleRows() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata nonUniqueTable = createTable(engine, "secondary-corruption.ibd", false);
            resolver.install(new UndoTargetMetadata(nonUniqueTable, Optional.empty()));
            LogicalRecord first = row(1, "duplicate@example.test");
            LogicalRecord second = row(2, "DUPLICATE@example.test");
            Transaction firstInsert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(firstInsert, nonUniqueTable, first,
                    Optional.empty(), TIMEOUT));
            commit(engine, firstInsert);
            Transaction secondInsert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(secondInsert, nonUniqueTable, second,
                    Optional.empty(), TIMEOUT));
            commit(engine, secondInsert);

            SecondaryIndexMetadata physical = nonUniqueTable.requireSecondary(EMAIL_ID);
            SecondaryIndexMetadata declaredUnique = new SecondaryIndexMetadata(
                    physical.index(), physical.layout(), true);
            TableIndexMetadata corruptedUniqueView = new TableIndexMetadata(nonUniqueTable.tableId(),
                    nonUniqueTable.schemaVersion(), nonUniqueTable.clusteredIndex(), List.of(declaredUnique));
            Transaction reader = transaction(engine);
            ReadView view = engine.transactionManager().readViewManager().openReadView(reader);

            assertThrows(SecondaryUniqueVisibilityCorruptionException.class,
                    () -> engine.secondaryMvccReader().readUnique(view, corruptedUniqueView,
                            declaredUnique, emailKey("duplicate@example.test")));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(reader));
        } finally {
            engine.close();
        }
    }

    /** 创建包含聚簇主键和大小写不敏感 unique email 的真实表。 */
    private TableIndexMetadata createTable(StorageEngine engine) {
        return createTable(engine, "secondary-mvcc.ibd", true);
    }

    /** 创建可选择 logical unique 属性的 email 二级索引，便于构造正常读和损坏检测输入。 */
    private TableIndexMetadata createTable(StorageEngine engine, String fileName, boolean logicalUnique) {
        StorageTableDefinition definition = new StorageTableDefinition(TABLE_ID, DATA_SPACE,
                directory.resolve(fileName), 1, PageNo.of(128),
                List.of(new StorageColumnDefinition(1, "id", 0,
                                StorageColumnType.bigint(false, false)),
                        new StorageColumnDefinition(2, "email", 1,
                                new StorageColumnType(StorageColumnTypeId.VARCHAR, false,
                                        160, 0, false, 1, 2, List.of()))),
                List.of(new StorageIndexDefinition(PRIMARY_ID, "PRIMARY", true, true,
                                List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))),
                        new StorageIndexDefinition(EMAIL_ID, logicalUnique ? "uq_email" : "idx_email",
                                logicalUnique, false,
                                List.of(new StorageIndexKeyPart(2, StorageIndexOrder.ASC, 0)))));
        TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition);
        return new BTreeIndexMetadataFactory().createTable(definition, binding);
    }

    /** 使用公开事务入口创建 RR 事务。 */
    private static Transaction transaction(StorageEngine engine) {
        return engine.transactionManager().begin(TransactionOptions.defaults());
    }

    /** 提交写事务并保持 redo durability 与生产 SQL 路径一致。 */
    private static void commit(StorageEngine engine, Transaction transaction) {
        engine.tableDmlService().commit(new DmlCommitCommand(transaction,
                DurabilityPolicy.FLUSH_ON_COMMIT, TIMEOUT));
    }

    /** 构造完整聚簇行。 */
    private static LogicalRecord row(long id, String email) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(email)), false, RecordType.CONVENTIONAL);
    }

    /** 构造聚簇主键。 */
    private static SearchKey primaryKey(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    /** 构造 logical unique email key；大小写等价性由索引 collation 处理。 */
    private static SearchKey emailKey(String email) {
        return new SearchKey(List.of(new ColumnValue.StringValue(email)));
    }

    /** open 前注入、DDL 后安装 exact-version 表聚合的测试 resolver。 */
    private static final class MutableTargetResolver
            implements IndexMetadataResolver, UndoTargetMetadataResolver {
        private volatile UndoTargetMetadata target;

        private void install(UndoTargetMetadata target) {
            this.target = target;
        }

        @Override
        public BTreeIndex resolve(long tableId, long indexId) {
            return requireTarget(tableId).tableIndexes().requireIndex(indexId);
        }

        @Override
        public UndoTargetMetadata resolveTarget(long tableId, long indexId) {
            UndoTargetMetadata resolved = requireTarget(tableId);
            if (resolved.clusteredIndex().indexId() != indexId) {
                throw new DatabaseValidationException("unexpected clustered index id: " + indexId);
            }
            return resolved;
        }

        private UndoTargetMetadata requireTarget(long tableId) {
            UndoTargetMetadata resolved = target;
            if (resolved == null || resolved.tableIndexes().tableId() != tableId) {
                throw new DatabaseValidationException("unknown test table: " + tableId);
            }
            return resolved;
        }
    }

    /** 构造独立 redo/undo/data 文件与足够的测试容量。 */
    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(30), 64L * 1024 * 1024);
    }
}
