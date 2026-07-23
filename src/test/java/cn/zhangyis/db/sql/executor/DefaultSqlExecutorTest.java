package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.domain.*;
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
import cn.zhangyis.db.sql.executor.storage.*;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.type.SqlValue;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.comparison;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.indexEqualityPredicates;

/** Executor 只做 sealed PhysicalPlan 的封闭分派与公开结果组装，不接收 Binder IR。 */
class DefaultSqlExecutorTest {
    /** INSERT 和 SELECT 都只通过 SQL storage port，不泄漏真实 Transaction/LogicalRecord。 */
    @Test
    void exhaustivelyExecutesInsertAndPrimaryPointSelect() {
        TableDefinition table = table();
        RecordingGateway gateway = new RecordingGateway();
        DefaultSqlExecutor executor = new DefaultSqlExecutor(gateway);
        TransactionStatus status = new TransactionStatus(false, true, false);
        SqlTransactionHandle handle = new TestHandle();

        UpdateResult update = assertInstanceOf(UpdateResult.class, executor.execute(handle,
                new PhysicalInsert(table, List.of(new SqlValue.IntegerValue(BigInteger.ONE),
                        new SqlValue.StringValue("before"))), status,
                SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals(1, update.affectedRows());

        List<SqlValue> primaryKey = List.of(
                new SqlValue.IntegerValue(BigInteger.ONE));
        QueryResult query = assertInstanceOf(QueryResult.class, executor.execute(handle,
                new PhysicalPointSelect(table, List.of(0), 3, PointAccessKind.CLUSTERED_PRIMARY,
                        primaryKey, indexEqualityPredicates(table, 3, primaryKey)), status,
                SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals("id", query.columns().getFirst().name());
        assertEquals(BigInteger.ONE, assertInstanceOf(SqlValue.IntegerValue.class,
                query.rows().getFirst().values().getFirst()).value());

        List<SqlValue> payloadKey = List.of(
                new SqlValue.StringValue("before"));
        QueryResult range = assertInstanceOf(QueryResult.class, executor.execute(handle,
                new PhysicalSecondaryRangeSelect(table, List.of(0), 4,
                        payloadKey, SelectLockMode.CONSISTENT,
                        indexEqualityPredicates(table, 4, payloadKey)),
                status, SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals(2, range.rows().size());
        UpdateResult changed = assertInstanceOf(UpdateResult.class, executor.execute(handle,
                new PhysicalPointUpdate(table, List.of(1), List.of(new SqlValue.StringValue("after")),
                        List.of(new SqlValue.IntegerValue(BigInteger.ONE))), status,
                SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals(1, changed.affectedRows());

        UpdateResult deleted = assertInstanceOf(UpdateResult.class, executor.execute(handle,
                new PhysicalPointDelete(table, List.of(new SqlValue.IntegerValue(BigInteger.ONE))), status,
                SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals(1, deleted.affectedRows());

        IndexRange payloadRange = new IndexRange(
                Optional.of(new RangeEndpoint(List.of(new SqlValue.StringValue("a")), true)),
                Optional.of(new RangeEndpoint(List.of(new SqlValue.StringValue("z")), false)));
        PredicateSet predicates = PredicateSet.of(comparison(
                table, 1, BoundComparisonOperator.GREATER_THAN_OR_EQUAL,
                new SqlValue.StringValue("a")));
        QueryResult comparison = assertInstanceOf(QueryResult.class, executor.execute(handle,
                new PhysicalRangeSelect(table, List.of(0), 4, payloadRange, predicates,
                        SelectLockMode.CONSISTENT, false),
                status, SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals(2, comparison.rows().size());
        assertEquals(2, assertInstanceOf(UpdateResult.class, executor.execute(handle,
                new PhysicalRangeUpdate(table, List.of(1), List.of(new SqlValue.StringValue("after")),
                        4, payloadRange, predicates, false),
                status, SqlStatementDeadline.after(Duration.ofSeconds(1)))).affectedRows());
        assertEquals(2, assertInstanceOf(UpdateResult.class, executor.execute(handle,
                new PhysicalRangeDelete(table, 4, payloadRange, predicates, false),
                status, SqlStatementDeadline.after(Duration.ofSeconds(1)))).affectedRows());
        assertEquals(8, gateway.calls);
    }

    private static TableDefinition table() {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        ColumnDefinition payload = new ColumnDefinition(2, ObjectName.of("payload"),
                new ColumnTypeDefinition(DictionaryTypeId.VARCHAR, false, false,
                        32, 0, 1, 1, List.of()), 1);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        IndexDefinition payloadIndex = new IndexDefinition(IndexId.of(4), ObjectName.of("idx_payload"),
                false, false, List.of(new IndexKeyPart(2, IndexOrder.ASC, 0)));
        SpaceId space = SpaceId.of(5);
        TableStorageBinding binding = new TableStorageBinding(2, space, Path.of("docs.ibd"), List.of(
                new IndexStorageBinding(3, PageId.of(space, PageNo.of(10)), 0,
                        new SegmentRef(space, 1, SegmentId.of(1)), new SegmentRef(space, 2, SegmentId.of(2))),
                new IndexStorageBinding(4, PageId.of(space, PageNo.of(11)), 0,
                        new SegmentRef(space, 3, SegmentId.of(3)), new SegmentRef(space, 4, SegmentId.of(4)))),
                Optional.empty());
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("docs"),
                DictionaryVersion.of(2), TableState.ACTIVE, List.of(id, payload),
                List.of(primary, payloadIndex), Optional.of(binding));
    }

    private static final class TestHandle implements SqlTransactionHandle { }

    private static final class RecordingGateway implements SqlStorageGateway {
        private int calls;
        @Override public SqlTransactionHandle begin(SqlTransactionRequest request) { return new TestHandle(); }
        @Override public SqlSavepointHandle createSavepoint(
                SqlTransactionHandle transaction, SqlStatementDeadline deadline) {
            return new TestSavepoint();
        }
        @Override public SqlSavepointHandle rollbackToSavepoint(
                SqlTransactionHandle transaction, SqlSavepointHandle savepoint,
                SqlStatementDeadline deadline) {
            return savepoint;
        }
        @Override public void releaseSavepoint(
                SqlTransactionHandle transaction, SqlSavepointHandle savepoint,
                SqlStatementDeadline deadline) {
        }
        @Override public SqlWriteOutcome insert(SqlTransactionHandle transaction, PhysicalInsert statement,
                                                SqlStatementDeadline deadline) {
            calls++; return new SqlWriteOutcome(1, false);
        }
        @Override public Optional<SqlRow> selectPoint(SqlTransactionHandle transaction,
                                                     PhysicalPointSelect statement,
                                                     SqlStatementDeadline deadline) {
            calls++; return Optional.of(new SqlRow(List.of(new SqlValue.IntegerValue(BigInteger.ONE))));
        }
        @Override public List<SqlRow> selectRange(SqlTransactionHandle transaction,
                                                 PhysicalSecondaryRangeSelect statement,
                                                 SqlStatementDeadline deadline) {
            calls++;
            return List.of(
                    new SqlRow(List.of(new SqlValue.IntegerValue(BigInteger.ONE))),
                    new SqlRow(List.of(new SqlValue.IntegerValue(BigInteger.TWO))));
        }
        @Override public List<SqlRow> selectRange(SqlTransactionHandle transaction,
                                                 PhysicalRangeSelect statement,
                                                 SqlStatementDeadline deadline) {
            calls++;
            return List.of(
                    new SqlRow(List.of(new SqlValue.IntegerValue(BigInteger.ONE))),
                    new SqlRow(List.of(new SqlValue.IntegerValue(BigInteger.TWO))));
        }
        @Override public SqlWriteOutcome update(SqlTransactionHandle transaction, PhysicalPointUpdate statement,
                                                SqlStatementDeadline deadline) {
            calls++; return new SqlWriteOutcome(1, false);
        }
        @Override public SqlWriteOutcome delete(SqlTransactionHandle transaction, PhysicalPointDelete statement,
                                                SqlStatementDeadline deadline) {
            calls++; return new SqlWriteOutcome(1, false);
        }
        @Override public SqlWriteOutcome updateRange(
                SqlTransactionHandle transaction, PhysicalRangeUpdate statement,
                SqlStatementDeadline deadline) {
            calls++; return new SqlWriteOutcome(2, false);
        }
        @Override public SqlWriteOutcome deleteRange(
                SqlTransactionHandle transaction, PhysicalRangeDelete statement,
                SqlStatementDeadline deadline) {
            calls++; return new SqlWriteOutcome(2, false);
        }
        @Override public SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request) {
            return new SqlCommitOutcome(0, true, 0);
        }
        @Override public SqlRollbackOutcome rollback(SqlTransactionHandle transaction) {
            return new SqlRollbackOutcome(0, 0);
        }
    }

    private static final class TestSavepoint implements SqlSavepointHandle { }
}
