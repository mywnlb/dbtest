package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.dd.ddl.*;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalInsert;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointSelect;
import cn.zhangyis.db.sql.optimizer.physical.PointAccessKind;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSecondaryRangeSelect;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeSelect;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeDelete;
import cn.zhangyis.db.sql.optimizer.physical.IndexRange;
import cn.zhangyis.db.sql.optimizer.physical.RangeEndpoint;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.expression.BoundNullTestOperator;
import cn.zhangyis.db.sql.executor.SqlRow;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.type.SqlValue;
import cn.zhangyis.db.sql.executor.storage.*;
import cn.zhangyis.db.sql.executor.storage.exception.SqlTransactionStateException;
import cn.zhangyis.db.storage.engine.EngineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.comparison;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.equal;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.indexEqualityPredicates;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.nullTest;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.or;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.predicates;

/** SQL port→真实 transaction/DML/MVCC/LOB 的 adapter 集成 TDD。 */
class DefaultSqlStorageGatewayTest {
    @TempDir Path directory;

    /** exact physical INSERT 真实维护全部索引，提交后唯一二级点查回表并按投影顺序 hydrate external TEXT。 */
    @Test
    void insertsCommitsAndReadsHydratedLobThroughOpaqueHandle() {
        try (DatabaseEngine database = openDatabase()) {
            TableDefinition table = createTable(database);
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(2));
            String text = "长".repeat(300);
            PhysicalInsert insert = new PhysicalInsert(table, List.of(
                    new SqlValue.IntegerValue(BigInteger.ONE), new SqlValue.StringValue("reader@example.test"),
                    new SqlValue.StringValue(text)));
            SqlTransactionHandle write = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            assertEquals(1, gateway.insert(write, insert,
                    SqlStatementDeadline.after(Duration.ofSeconds(2))).affectedRows());
            assertTrue(gateway.commit(write, new SqlCommitRequest(SqlDurabilityMode.FLUSH_ON_COMMIT,
                    Duration.ofSeconds(2))).durable());
            assertThrows(SqlTransactionStateException.class, () -> gateway.rollback(write));

            SqlTransactionHandle read = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.READ_COMMITTED, true, true));
            IndexDefinition email = table.indexes().stream().filter(index -> !index.clustered())
                    .findFirst().orElseThrow();
            PhysicalPointSelect select = pointSelect(table, List.of(2, 0), email.id().value(),
                    PointAccessKind.UNIQUE_SECONDARY,
                    List.of(new SqlValue.StringValue("READER@example.test")));
            SqlRow row = gateway.selectPoint(read, select,
                    SqlStatementDeadline.after(Duration.ofSeconds(2))).orElseThrow();
            assertEquals(List.of(new SqlValue.StringValue(text),
                    new SqlValue.IntegerValue(BigInteger.ONE)), row.values());
            gateway.commit(read, new SqlCommitRequest(SqlDurabilityMode.BACKGROUND_FLUSH,
                    Duration.ofSeconds(1)));
        }
    }

    /**
     * Opaque gateway XA 能力必须保持真实 Transaction 隔离，并完成 write identity、phase one、
     * durable prepared commit 与 terminal handle 拒绝。
     */
    @Test
    void preparesAndCommitsWrittenOpaqueXaHandle() {
        try (DatabaseEngine database = openDatabase()) {
            TableDefinition table = createTable(database);
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(2));
            SqlTransactionHandle handle = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            gateway.insert(handle, new PhysicalInsert(table, List.of(
                            new SqlValue.IntegerValue(BigInteger.valueOf(91)),
                            new SqlValue.StringValue("xa@example.test"),
                            new SqlValue.StringValue("prepared"))),
                    SqlStatementDeadline.after(Duration.ofSeconds(2)));

            SqlXaTransactionIdentity identity = gateway.xaIdentity(handle);
            assertTrue(identity.hasWrites());
            assertEquals(identity.transactionId(),
                    gateway.prepareXa(handle, Duration.ofSeconds(2)).transactionId());
            SqlXaCompletionOutcome completed =
                    gateway.commitPreparedXa(handle, Duration.ofSeconds(2));
            assertTrue(completed.committed());
            assertTrue(completed.transactionNumber() > 0);
            assertThrows(SqlTransactionStateException.class, () -> gateway.rollback(handle));
        }
    }

    /** point UPDATE/DELETE 复用同一 statement guard，维护 unique secondary 并替换 external LOB。 */
    @Test
    void updatesAndDeletesThroughTypedPointWritePort() {
        try (DatabaseEngine database = openDatabase()) {
            TableDefinition table = createTable(database);
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(2));
            String before = "旧".repeat(300);
            String after = "新".repeat(320);
            SqlTransactionHandle insertTxn = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            gateway.insert(insertTxn, new PhysicalInsert(table, List.of(
                    new SqlValue.IntegerValue(BigInteger.ONE),
                    new SqlValue.StringValue("before@example.test"),
                    new SqlValue.StringValue(before))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5)));
            gateway.commit(insertTxn, new SqlCommitRequest(SqlDurabilityMode.FLUSH_ON_COMMIT,
                    Duration.ofSeconds(2)));

            SqlTransactionHandle updateTxn = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            SqlWriteOutcome updated = gateway.update(updateTxn, new PhysicalPointUpdate(table, List.of(1, 2),
                    List.of(new SqlValue.StringValue("after@example.test"), new SqlValue.StringValue(after)),
                    List.of(new SqlValue.IntegerValue(BigInteger.ONE))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5)));
            assertEquals(1, updated.affectedRows());
            gateway.commit(updateTxn, new SqlCommitRequest(SqlDurabilityMode.FLUSH_ON_COMMIT,
                    Duration.ofSeconds(2)));

            IndexDefinition email = table.indexes().stream().filter(index -> !index.clustered())
                    .findFirst().orElseThrow();
            SqlTransactionHandle read = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.READ_COMMITTED, true, true));
            assertTrue(gateway.selectPoint(read, pointSelect(table, List.of(2), email.id().value(),
                    PointAccessKind.UNIQUE_SECONDARY,
                    List.of(new SqlValue.StringValue("BEFORE@example.test"))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5))).isEmpty());
            assertEquals(new SqlValue.StringValue(after), gateway.selectPoint(read,
                    pointSelect(table, List.of(2), email.id().value(),
                            PointAccessKind.UNIQUE_SECONDARY,
                            List.of(new SqlValue.StringValue("AFTER@example.test"))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5)))
                    .orElseThrow().values().getFirst());
            gateway.commit(read, new SqlCommitRequest(SqlDurabilityMode.BACKGROUND_FLUSH,
                    Duration.ofSeconds(1)));

            SqlTransactionHandle deleteTxn = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            assertEquals(1, gateway.delete(deleteTxn, new PhysicalPointDelete(table,
                            List.of(new SqlValue.IntegerValue(BigInteger.ONE))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5))).affectedRows());
            gateway.commit(deleteTxn, new SqlCommitRequest(SqlDurabilityMode.FLUSH_ON_COMMIT,
                    Duration.ofSeconds(2)));

            SqlTransactionHandle verify = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.READ_COMMITTED, true, true));
            assertTrue(gateway.selectPoint(verify, pointSelect(table, List.of(0), email.id().value(),
                    PointAccessKind.UNIQUE_SECONDARY,
                    List.of(new SqlValue.StringValue("AFTER@example.test"))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5))).isEmpty());
            gateway.commit(verify, new SqlCommitRequest(SqlDurabilityMode.BACKGROUND_FLUSH,
                    Duration.ofSeconds(1)));
        }
    }

    /** non-unique logical key 返回多行；consistent range 覆盖 LOB hydrate，locking range 持锁到 opaque commit。 */
    @Test
    void readsNonUniqueSecondaryRangeAndCommitsLockingRead() {
        try (DatabaseEngine database = openDatabase()) {
            TableDefinition table = createRangeTable(database);
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(2));
            String firstBody = "甲".repeat(300);
            String secondBody = "乙".repeat(320);
            for (int id = 1; id <= 2; id++) {
                SqlTransactionHandle insert = gateway.begin(new SqlTransactionRequest(
                        SqlIsolationLevel.REPEATABLE_READ, false, false));
                gateway.insert(insert, new PhysicalInsert(table, List.of(
                                new SqlValue.IntegerValue(BigInteger.valueOf(id)),
                                new SqlValue.StringValue(id == 1 ? "team@example.test" : "TEAM@example.test"),
                                new SqlValue.StringValue(id == 1 ? firstBody : secondBody))),
                        SqlStatementDeadline.after(Duration.ofSeconds(5)));
                gateway.commit(insert, new SqlCommitRequest(SqlDurabilityMode.FLUSH_ON_COMMIT,
                        Duration.ofSeconds(2)));
            }
            IndexDefinition email = table.indexes().stream().filter(index -> !index.clustered())
                    .findFirst().orElseThrow();
            PhysicalSecondaryRangeSelect consistent = new PhysicalSecondaryRangeSelect(table, List.of(2, 0),
                    email.id().value(), List.of(new SqlValue.StringValue("team@example.test")),
                    SelectLockMode.CONSISTENT,
                    indexEqualityPredicates(table, email.id().value(),
                            List.of(new SqlValue.StringValue("team@example.test"))));
            SqlTransactionHandle read = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.READ_COMMITTED, true, true));
            assertEquals(List.of(
                    new SqlRow(List.of(new SqlValue.StringValue(firstBody),
                            new SqlValue.IntegerValue(BigInteger.ONE))),
                    new SqlRow(List.of(new SqlValue.StringValue(secondBody),
                            new SqlValue.IntegerValue(BigInteger.valueOf(2))))),
                    gateway.selectRange(read, consistent, SqlStatementDeadline.after(Duration.ofSeconds(5))));
            gateway.commit(read, new SqlCommitRequest(SqlDurabilityMode.BACKGROUND_FLUSH,
                    Duration.ofSeconds(1)));

            SqlTransactionHandle locking = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            PhysicalSecondaryRangeSelect forShare = new PhysicalSecondaryRangeSelect(table, List.of(0),
                    email.id().value(), List.of(new SqlValue.StringValue("TEAM@example.test")),
                    SelectLockMode.FOR_SHARE,
                    indexEqualityPredicates(table, email.id().value(),
                            List.of(new SqlValue.StringValue("TEAM@example.test"))));
            assertEquals(2, gateway.selectRange(locking, forShare,
                    SqlStatementDeadline.after(Duration.ofSeconds(5))).size());
            assertTrue(gateway.commit(locking, new SqlCommitRequest(
                    SqlDurabilityMode.BACKGROUND_FLUSH, Duration.ofSeconds(1))).releasedLockCount() >= 3);
        }
    }

    /**
     * comparison range 必须走真实分页/current-read；范围 UPDATE 先物化 identity 后再修改
     * access key，确保不会因新 key 再次进入扫描而发生 Halloween，范围 DELETE 同样一次性收口。
     */
    @Test
    void executesComparisonFullScanAndRangeMutationsWithoutHalloween() {
        try (DatabaseEngine database = openDatabase()) {
            TableDefinition table = createRangeTable(database);
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(3));
            for (int id = 1; id <= 3; id++) {
                SqlTransactionHandle insert = gateway.begin(new SqlTransactionRequest(
                        SqlIsolationLevel.REPEATABLE_READ, false, false));
                gateway.insert(insert, new PhysicalInsert(table, List.of(
                                new SqlValue.IntegerValue(BigInteger.valueOf(id)),
                                new SqlValue.StringValue("m" + id + "@example.test"),
                                new SqlValue.StringValue("body-" + id))),
                        SqlStatementDeadline.after(Duration.ofSeconds(5)));
                gateway.commit(insert, new SqlCommitRequest(
                        SqlDurabilityMode.FLUSH_ON_COMMIT, Duration.ofSeconds(2)));
            }
            IndexDefinition email = table.indexes().stream()
                    .filter(index -> !index.clustered()).findFirst().orElseThrow();
            IndexRange emailRange = new IndexRange(
                    java.util.Optional.of(new RangeEndpoint(
                            List.of(new SqlValue.StringValue("a")), true)),
                    java.util.Optional.of(new RangeEndpoint(
                            List.of(new SqlValue.StringValue("z")), false)));
            PredicateSet emailPredicates = predicates(
                    comparison(table, 1,
                            BoundComparisonOperator.GREATER_THAN_OR_EQUAL,
                            new SqlValue.StringValue("a")),
                    comparison(table, 1, BoundComparisonOperator.LESS_THAN,
                            new SqlValue.StringValue("z")));

            SqlTransactionHandle update = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            SqlWriteOutcome changed = gateway.updateRange(update, new PhysicalRangeUpdate(
                            table, List.of(1), List.of(new SqlValue.StringValue("zz@example.test")),
                            email.id().value(), emailRange, emailPredicates, false),
                    SqlStatementDeadline.after(Duration.ofSeconds(10)));
            assertEquals(3, changed.affectedRows(),
                    "每个初始 identity 只能修改一次，不能因索引 key 变化重复进入扫描");
            gateway.commit(update, new SqlCommitRequest(
                    SqlDurabilityMode.FLUSH_ON_COMMIT, Duration.ofSeconds(3)));

            SqlTransactionHandle fullScan = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.READ_COMMITTED, true, true));
            PhysicalRangeSelect scan = new PhysicalRangeSelect(table, List.of(0),
                    table.primaryIndex().id().value(), IndexRange.unbounded(),
                    predicates(comparison(table, 1,
                            BoundComparisonOperator.EQUAL,
                            new SqlValue.StringValue("ZZ@example.test"))),
                    SelectLockMode.CONSISTENT, false);
            assertEquals(List.of(BigInteger.ONE, BigInteger.TWO, BigInteger.valueOf(3)),
                    gateway.selectRange(fullScan, scan, SqlStatementDeadline.after(Duration.ofSeconds(5)))
                            .stream()
                            .map(row -> assertInstanceOf(
                                    SqlValue.IntegerValue.class, row.values().getFirst()).value())
                            .toList());
            gateway.commit(fullScan, new SqlCommitRequest(
                    SqlDurabilityMode.BACKGROUND_FLUSH, Duration.ofSeconds(1)));

            IndexRange idRange = new IndexRange(
                    java.util.Optional.of(new RangeEndpoint(
                            List.of(new SqlValue.IntegerValue(BigInteger.TWO)), true)),
                    java.util.Optional.of(new RangeEndpoint(
                            List.of(new SqlValue.IntegerValue(BigInteger.valueOf(3))), true)));
            PredicateSet idPredicates = predicates(
                    comparison(table, 0,
                            BoundComparisonOperator.GREATER_THAN_OR_EQUAL,
                            new SqlValue.IntegerValue(BigInteger.TWO)),
                    comparison(table, 0,
                            BoundComparisonOperator.LESS_THAN_OR_EQUAL,
                            new SqlValue.IntegerValue(BigInteger.valueOf(3))));
            SqlTransactionHandle delete = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            assertEquals(2, gateway.deleteRange(delete, new PhysicalRangeDelete(
                            table, table.primaryIndex().id().value(), idRange, idPredicates, false),
                    SqlStatementDeadline.after(Duration.ofSeconds(10))).affectedRows());
            gateway.commit(delete, new SqlCommitRequest(
                    SqlDurabilityMode.FLUSH_ON_COMMIT, Duration.ofSeconds(3)));

            SqlTransactionHandle verify = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.READ_COMMITTED, true, true));
            assertEquals(1, gateway.selectRange(verify, scan,
                    SqlStatementDeadline.after(Duration.ofSeconds(5))).size());
            gateway.commit(verify, new SqlCommitRequest(
                    SqlDurabilityMode.BACKGROUND_FLUSH, Duration.ofSeconds(1)));
        }
    }

    /**
     * RU 不创建 ReadView：另一 ACTIVE 事务的最新未标删版本立即可见；当前 delete-mark 也立即使该行消失。
     * 聚簇与唯一二级访问都必须回到同一当前版本语义。
     */
    @Test
    void readUncommittedSeesLatestUncommittedVersionAndCurrentDeleteMark() {
        try (DatabaseEngine database = openDatabase()) {
            TableDefinition table = createTable(database);
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(3));
            SqlTransactionHandle writer = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            gateway.insert(writer, new PhysicalInsert(table, List.of(
                            new SqlValue.IntegerValue(BigInteger.ONE),
                            new SqlValue.StringValue("dirty@example.test"),
                            new SqlValue.StringValue("uncommitted"))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5)));

            SqlTransactionHandle ru = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.READ_UNCOMMITTED, true, true));
            PhysicalPointSelect primary = pointSelect(table, List.of(2),
                    table.primaryIndex().id().value(), PointAccessKind.CLUSTERED_PRIMARY,
                    List.of(new SqlValue.IntegerValue(BigInteger.ONE)));
            assertEquals(new SqlValue.StringValue("uncommitted"),
                    gateway.selectPoint(ru, primary,
                            SqlStatementDeadline.after(Duration.ofSeconds(5)))
                            .orElseThrow().values().getFirst());
            List<SqlValue> primaryKey = List.of(
                    new SqlValue.IntegerValue(BigInteger.ONE));
            PhysicalPointSelect rejectedByResidual =
                    new PhysicalPointSelect(
                            table, List.of(2),
                            table.primaryIndex().id().value(),
                            PointAccessKind.CLUSTERED_PRIMARY,
                            primaryKey,
                            predicates(
                                    equal(table, 0, primaryKey.getFirst()),
                                    equal(table, 1,
                                            new SqlValue.StringValue(
                                                    "different"))));
            assertTrue(gateway.selectPoint(
                    ru, rejectedByResidual,
                    SqlStatementDeadline.after(
                            Duration.ofSeconds(5))).isEmpty(),
                    "RU 聚簇 access key 命中后仍必须执行完整 residual");
            PhysicalPointSelect acceptedByBooleanResidual =
                    new PhysicalPointSelect(
                            table, List.of(2),
                            table.primaryIndex().id().value(),
                            PointAccessKind.CLUSTERED_PRIMARY,
                            primaryKey,
                            predicates(
                                    equal(table, 0,
                                            primaryKey.getFirst()),
                                    or(equal(
                                                    table, 1,
                                                    new SqlValue.StringValue(
                                                            "different")),
                                            nullTest(
                                                    table, 2,
                                                    BoundNullTestOperator
                                                            .IS_NOT_NULL))));
            assertEquals(new SqlValue.StringValue("uncommitted"),
                    gateway.selectPoint(
                            ru, acceptedByBooleanResidual,
                            SqlStatementDeadline.after(
                                    Duration.ofSeconds(5)))
                            .orElseThrow().values().getFirst(),
                    "RU point 命中后必须以同一当前版本求值 OR/null-test residual");
            IndexDefinition email = table.indexes().stream()
                    .filter(index -> !index.clustered()).findFirst().orElseThrow();
            PhysicalPointSelect secondary = pointSelect(table, List.of(0),
                    email.id().value(), PointAccessKind.UNIQUE_SECONDARY,
                    List.of(new SqlValue.StringValue("DIRTY@example.test")));
            assertEquals(new SqlValue.IntegerValue(BigInteger.ONE),
                    gateway.selectPoint(ru, secondary,
                            SqlStatementDeadline.after(Duration.ofSeconds(5)))
                            .orElseThrow().values().getFirst());

            gateway.delete(writer, new PhysicalPointDelete(table,
                            List.of(new SqlValue.IntegerValue(BigInteger.ONE))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5)));
            assertTrue(gateway.selectPoint(ru, primary,
                    SqlStatementDeadline.after(Duration.ofSeconds(5))).isEmpty());
            assertTrue(gateway.selectPoint(ru, secondary,
                    SqlStatementDeadline.after(Duration.ofSeconds(5))).isEmpty());

            gateway.rollback(writer);
            gateway.commit(ru, new SqlCommitRequest(
                    SqlDurabilityMode.BACKGROUND_FLUSH, Duration.ofSeconds(1)));
        }
    }

    /**
     * opaque 保存点必须撤销更晚 undo、保留目标供重复 ROLLBACK TO，并在 RELEASE 后拒绝复用旧能力。
     */
    @Test
    void rollsBackAndReusesOpaqueSqlSavepoint() {
        try (DatabaseEngine database = openDatabase()) {
            TableDefinition table = createTable(database);
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(3));
            SqlTransactionHandle transaction = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            gateway.insert(transaction, new PhysicalInsert(table, List.of(
                            new SqlValue.IntegerValue(BigInteger.ONE),
                            new SqlValue.StringValue("first@example.test"),
                            new SqlValue.StringValue("first"))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5)));
            SqlStatementDeadline savepointDeadline =
                    SqlStatementDeadline.after(Duration.ofSeconds(10));
            SqlSavepointHandle savepoint =
                    gateway.createSavepoint(transaction, savepointDeadline);
            gateway.insert(transaction, new PhysicalInsert(table, List.of(
                            new SqlValue.IntegerValue(BigInteger.TWO),
                            new SqlValue.StringValue("second@example.test"),
                            new SqlValue.StringValue("second"))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5)));

            savepoint = gateway.rollbackToSavepoint(
                    transaction, savepoint, savepointDeadline);
            savepoint = gateway.rollbackToSavepoint(
                    transaction, savepoint, savepointDeadline);
            gateway.releaseSavepoint(transaction, savepoint, savepointDeadline);
            SqlSavepointHandle released = savepoint;
            assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                    () -> gateway.releaseSavepoint(
                            transaction, released, savepointDeadline));
            gateway.commit(transaction, new SqlCommitRequest(
                    SqlDurabilityMode.FLUSH_ON_COMMIT, Duration.ofSeconds(3)));

            SqlTransactionHandle verify = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.READ_COMMITTED, true, true));
            assertTrue(gateway.selectPoint(verify, pointSelect(table, List.of(0),
                            table.primaryIndex().id().value(), PointAccessKind.CLUSTERED_PRIMARY,
                            List.of(new SqlValue.IntegerValue(BigInteger.ONE))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5))).isPresent());
            assertTrue(gateway.selectPoint(verify, pointSelect(table, List.of(0),
                            table.primaryIndex().id().value(), PointAccessKind.CLUSTERED_PRIMARY,
                            List.of(new SqlValue.IntegerValue(BigInteger.TWO))),
                    SqlStatementDeadline.after(Duration.ofSeconds(5))).isEmpty());
            gateway.commit(verify, new SqlCommitRequest(
                    SqlDurabilityMode.BACKGROUND_FLUSH, Duration.ofSeconds(1)));
        }
    }

    /** 无 undo 事务可直接 rollback；跨 gateway、重复终结和终态 handle 必须 fail-closed。 */
    @Test
    void validatesHandleOwnershipAndTerminalState() {
        try (DatabaseEngine database = openDatabase()) {
            DefaultSqlStorageGateway first = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(1));
            DefaultSqlStorageGateway second = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(1));
            SqlTransactionHandle handle = first.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            assertThrows(SqlTransactionStateException.class, () -> second.rollback(handle));
            assertEquals(0, first.rollback(handle).undoneRecords());
            assertThrows(SqlTransactionStateException.class, () -> first.rollback(handle));
            assertThrows(SqlTransactionStateException.class, () -> first.commit(handle,
                    new SqlCommitRequest(SqlDurabilityMode.BACKGROUND_FLUSH, Duration.ofSeconds(1))));
        }
    }

    /** statement 的绝对 deadline 必须截断 gateway 自己更长的 handle wait，不能在前序阶段耗时后重新计时。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void statementDeadlineCapsTransactionHandleWait() throws Exception {
        try (DatabaseEngine database = openDatabase();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            TableDefinition table = createTable(database);
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(2));
            EngineSqlTransactionHandle handle = (EngineSqlTransactionHandle) gateway.begin(
                    new SqlTransactionRequest(SqlIsolationLevel.READ_COMMITTED, true, true));
            CountDownLatch locked = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            var holder = executor.submit(() -> {
                handle.operationLock.lock();
                try {
                    locked.countDown();
                    assertTrue(release.await(2, TimeUnit.SECONDS));
                } finally {
                    handle.operationLock.unlock();
                }
                return null;
            });
            assertTrue(locked.await(1, TimeUnit.SECONDS));
            PhysicalPointSelect select = pointSelect(table, List.of(0),
                    table.primaryIndex().id().value(), PointAccessKind.CLUSTERED_PRIMARY,
                    List.of(new SqlValue.IntegerValue(BigInteger.ONE)));
            long started = System.nanoTime();
            assertThrows(SqlTransactionStateException.class, () -> gateway.selectPoint(handle, select,
                    SqlStatementDeadline.after(Duration.ofMillis(80))));
            Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
            assertTrue(elapsed.compareTo(Duration.ofMillis(500)) < 0,
                    () -> "statement deadline was not honored, elapsed=" + elapsed);
            release.countDown();
            holder.get(1, TimeUnit.SECONDS);
            gateway.rollback(handle);
        }
    }

    private DatabaseEngine openDatabase() {
        DatabaseEngine database = new DatabaseEngine(new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(10), 64L * 1024 * 1024));
        database.open();
        return database;
    }

    /**
     * 为 adapter 集成测试构造携带完整等值残余条件的 point plan。
     *
     * @param table exact DD 表定义
     * @param projectionOrdinals 返回列位置
     * @param indexId 稳定访问索引 id
     * @param accessKind 聚簇或唯一二级访问类别
     * @param keyValues 按 logical index key part 排列的完整 key
     * @return 同时包含 range 缩小条件与最终 SQL truth 条件的 point plan
     */
    private static PhysicalPointSelect pointSelect(
            TableDefinition table,
            List<Integer> projectionOrdinals,
            long indexId,
            PointAccessKind accessKind,
            List<SqlValue> keyValues) {
        return new PhysicalPointSelect(
                table, projectionOrdinals, indexId, accessKind, keyValues,
                indexEqualityPredicates(table, indexId, keyValues));
    }

    private static TableDefinition createTable(DatabaseEngine database) {
        database.ddl().createSchema(MdlOwnerId.of(300), ObjectName.of("app"), 1, 1, Duration.ofSeconds(2));
        TableDefinition created = database.ddl().createTable(MdlOwnerId.of(300), new CreateTableCommand(
                QualifiedTableName.of("app", "docs"), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false)),
                        new CreateColumnSpec(ObjectName.of("email"), new ColumnTypeDefinition(
                                DictionaryTypeId.VARCHAR, false, false, 160, 0, 1, 2, List.of())),
                        new CreateColumnSpec(ObjectName.of("body"), new ColumnTypeDefinition(DictionaryTypeId.TEXT,
                                false, false, 65_535, 0, 1, 1, List.of()))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                                List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))),
                        new CreateIndexSpec(ObjectName.of("uq_email"), true, false,
                                List.of(new CreateIndexKeyPartSpec(ObjectName.of("email"), IndexOrder.ASC, 0))))),
                Duration.ofSeconds(5));
        try (var lease = database.dictionary().openTable(MdlOwnerId.of(301), QualifiedTableName.of("app", "docs"),
                TableAccessIntent.WRITE, Duration.ofSeconds(2))) {
            assertEquals(created.version(), lease.version());
            return lease.table();
        }
    }

    /** 创建 ordinary non-unique email 索引，完整 logical key 在 physical tree 中形成多行 prefix range。 */
    private static TableDefinition createRangeTable(DatabaseEngine database) {
        database.ddl().createSchema(MdlOwnerId.of(310), ObjectName.of("app"), 1, 1, Duration.ofSeconds(2));
        TableDefinition created = database.ddl().createTable(MdlOwnerId.of(310), new CreateTableCommand(
                QualifiedTableName.of("app", "range_docs"), PageNo.of(160),
                List.of(new CreateColumnSpec(ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false)),
                        new CreateColumnSpec(ObjectName.of("email"), new ColumnTypeDefinition(
                                DictionaryTypeId.VARCHAR, false, false, 160, 0, 1, 2, List.of())),
                        new CreateColumnSpec(ObjectName.of("body"), new ColumnTypeDefinition(DictionaryTypeId.TEXT,
                                false, false, 65_535, 0, 1, 1, List.of()))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                                List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))),
                        new CreateIndexSpec(ObjectName.of("idx_email"), false, false,
                                List.of(new CreateIndexKeyPartSpec(ObjectName.of("email"), IndexOrder.ASC, 0))))),
                Duration.ofSeconds(5));
        try (var lease = database.dictionary().openTable(MdlOwnerId.of(311),
                QualifiedTableName.of("app", "range_docs"),
                TableAccessIntent.WRITE, Duration.ofSeconds(2))) {
            assertEquals(created.version(), lease.version());
            return lease.table();
        }
    }
}
