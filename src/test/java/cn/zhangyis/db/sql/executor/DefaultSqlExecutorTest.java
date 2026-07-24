package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.domain.*;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalInsert;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalFilter;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointAccess;
import cn.zhangyis.db.sql.optimizer.physical.PointAccessKind;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalProject;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalQuery;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSecondaryPrefixAccess;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeDelete;
import cn.zhangyis.db.sql.optimizer.physical.IndexRange;
import cn.zhangyis.db.sql.optimizer.physical.RangeEndpoint;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.executor.exception.SqlExecutionException;
import cn.zhangyis.db.sql.executor.expression.ExpressionEvaluator;
import cn.zhangyis.db.sql.executor.node.PlanNode;
import cn.zhangyis.db.sql.executor.node.PlanNodeFactory;
import cn.zhangyis.db.sql.executor.node.PlanNodeState;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
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
                query(table, List.of(0),
                        new PhysicalPointAccess(table, 3,
                                PointAccessKind.CLUSTERED_PRIMARY,
                                primaryKey),
                        indexEqualityPredicates(table, 3, primaryKey)), status,
                SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals("id", query.columns().getFirst().name());
        assertEquals(BigInteger.ONE, assertInstanceOf(SqlValue.IntegerValue.class,
                query.rows().getFirst().values().getFirst()).value());

        List<SqlValue> payloadKey = List.of(
                new SqlValue.StringValue("before"));
        QueryResult range = assertInstanceOf(QueryResult.class, executor.execute(handle,
                query(table, List.of(0),
                        new PhysicalSecondaryPrefixAccess(
                                table, 4, payloadKey,
                                SelectLockMode.CONSISTENT),
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
                query(table, List.of(0),
                        new PhysicalRangeAccess(
                                table, 4, payloadRange,
                                SelectLockMode.CONSISTENT, false),
                        predicates),
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

    /** PlanNode 必须显式经历 NEW→OPEN→EXHAUSTED→CLOSED，并使越过 advance 的旧视图失效。 */
    @Test
    void enforcesPullNodeLifecycleAndCursorViewLifetime() {
        TableDefinition table = table();
        RecordingGateway gateway = new RecordingGateway();
        List<SqlValue> key = List.of(
                new SqlValue.IntegerValue(BigInteger.ONE));
        PlanNode root = new PlanNodeFactory(
                gateway, new ExpressionEvaluator()).create(
                query(table, List.of(1, 0),
                        new PhysicalPointAccess(
                                table, 3,
                                PointAccessKind.CLUSTERED_PRIMARY,
                                key),
                        indexEqualityPredicates(table, 3, key)));

        assertEquals(PlanNodeState.NEW, root.state());
        assertThrows(SqlExecutionException.class, root::advance);
        root.open(new ExecutionContext(
                new TestHandle(),
                SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals(PlanNodeState.OPEN, root.state());
        assertThrows(SqlExecutionException.class, root::current);

        assertTrue(root.advance());
        SqlRowView first = root.current();
        assertEquals(new SqlValue.StringValue("before"),
                first.valueAt(0));
        assertFalse(root.advance());
        assertEquals(PlanNodeState.EXHAUSTED, root.state());
        assertThrows(AssertionError.class, () -> first.valueAt(0));
        assertThrows(SqlExecutionException.class, root::current);
        assertFalse(root.advance());

        root.close();
        root.close();
        assertEquals(PlanNodeState.CLOSED, root.state());
        assertEquals(1, gateway.cursorCloses);
    }

    /** 拉取失败时 Executor 必须关闭 cursor，并把 close 失败挂到主异常而不覆盖根因。 */
    @Test
    void closesCursorAndSuppressesCloseFailureAfterPullFailure() {
        TableDefinition table = table();
        RecordingGateway gateway = new RecordingGateway();
        gateway.advanceFailure = new SqlExecutionException(
                "test cursor advance failed");
        gateway.closeFailure = new SqlExecutionException(
                "test cursor close failed");
        List<SqlValue> key = List.of(
                new SqlValue.IntegerValue(BigInteger.ONE));

        SqlExecutionException failure = assertThrows(
                SqlExecutionException.class,
                () -> new DefaultSqlExecutor(gateway).execute(
                        new TestHandle(),
                        query(table, List.of(0),
                                new PhysicalPointAccess(
                                        table, 3,
                                        PointAccessKind.CLUSTERED_PRIMARY,
                                        key),
                                indexEqualityPredicates(
                                        table, 3, key)),
                        new TransactionStatus(false, true, false),
                        SqlStatementDeadline.after(
                                Duration.ofSeconds(1))));

        assertSame(gateway.advanceFailure, failure);
        assertEquals(1, gateway.cursorCloses);
        assertEquals(1, failure.getSuppressed().length);
        assertSame(gateway.closeFailure,
                failure.getSuppressed()[0]);
    }

    /** Filter 后第 4097 行必须 fail-closed，且容量异常仍关闭底层 cursor。 */
    @Test
    void rejectsOversizedEagerResultAndClosesCursor() {
        TableDefinition table = table();
        RecordingGateway gateway = new RecordingGateway();
        gateway.rangeRowCount = 4097;
        PredicateSet predicates = PredicateSet.of(comparison(
                table, 0,
                BoundComparisonOperator.GREATER_THAN_OR_EQUAL,
                new SqlValue.IntegerValue(BigInteger.ZERO)));

        SqlExecutionException failure = assertThrows(
                SqlExecutionException.class,
                () -> new DefaultSqlExecutor(gateway).execute(
                        new TestHandle(),
                        query(table, List.of(0),
                                new PhysicalRangeAccess(
                                        table, 3,
                                        IndexRange.unbounded(),
                                        SelectLockMode.CONSISTENT,
                                        false),
                                predicates),
                        new TransactionStatus(false, true, false),
                        SqlStatementDeadline.after(
                                Duration.ofSeconds(2))));

        assertTrue(failure.getMessage().contains(
                "query result row limit exceeded"));
        assertEquals(1, gateway.cursorCloses);
    }

    /**
     * 按 M4 唯一权威形状构造物理查询，测试不绕过 Filter/Project。
     */
    private static PhysicalQuery query(
            TableDefinition table, List<Integer> projections,
            PhysicalAccess access, PredicateSet predicates) {
        assertSame(table, access.table());
        return new PhysicalQuery(new PhysicalProject(
                new PhysicalFilter(access, predicates),
                projections));
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
        private int cursorCloses;
        private RuntimeException advanceFailure;
        private RuntimeException closeFailure;
        private int rangeRowCount = 2;
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
        @Override public SqlStorageCursor openCursor(
                SqlTransactionHandle transaction,
                PhysicalAccess access,
                SqlStatementDeadline deadline) {
            calls++;
            List<SqlRow> rows = access instanceof PhysicalPointAccess
                    ? List.of(row(1, "before"))
                    : java.util.stream.LongStream.rangeClosed(
                                    1, rangeRowCount)
                            .mapToObj(id -> row(id, "before"))
                            .toList();
            return new TestCursor(rows, this);
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

    /** 构造访问叶输出的完整 table row，而非已投影公开行。 */
    private static SqlRow row(long id, String payload) {
        return new SqlRow(List.of(
                new SqlValue.IntegerValue(BigInteger.valueOf(id)),
                new SqlValue.StringValue(payload)));
    }

    /** 测试 Data Port cursor；advance 后旧 row view 失效，close 幂等。 */
    private static final class TestCursor implements SqlStorageCursor {
        private final List<SqlRow> rows;
        private final RecordingGateway owner;
        private int index = -1;
        private boolean closed;

        private TestCursor(
                List<SqlRow> rows, RecordingGateway owner) {
            this.rows = List.copyOf(rows);
            this.owner = owner;
        }

        @Override
        public boolean advance() {
            if (closed) throw new AssertionError("advance after close");
            if (owner.advanceFailure != null) {
                throw owner.advanceFailure;
            }
            index++;
            return index < rows.size();
        }

        @Override
        public SqlRowView current() {
            if (closed || index < 0 || index >= rows.size()) {
                throw new AssertionError("cursor has no current row");
            }
            SqlRow row = rows.get(index);
            int generation = index;
            return new SqlRowView() {
                private void valid() {
                    if (closed || index != generation) {
                        throw new AssertionError("stale test row view");
                    }
                }
                @Override public int width() { valid(); return row.values().size(); }
                @Override public SqlValue valueAt(int ordinal) {
                    valid(); return row.values().get(ordinal);
                }
                @Override public boolean isNullAt(int ordinal) {
                    valid();
                    return row.values().get(ordinal)
                            instanceof SqlValue.NullValue;
                }
                @Override public int compareLiteral(int ordinal, SqlValue literal) {
                    valid();
                    SqlValue value = row.values().get(ordinal);
                    if (value instanceof SqlValue.IntegerValue left
                            && literal instanceof SqlValue.IntegerValue right) {
                        return left.value().compareTo(right.value());
                    }
                    if (value instanceof SqlValue.StringValue left
                            && literal instanceof SqlValue.StringValue right) {
                        return left.value().compareTo(right.value());
                    }
                    throw new AssertionError("unsupported test comparison");
                }
            };
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            owner.cursorCloses++;
            if (owner.closeFailure != null) {
                throw owner.closeFailure;
            }
        }
    }
}
