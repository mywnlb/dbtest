package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.domain.*;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.PointAccessKind;
import cn.zhangyis.db.sql.executor.storage.*;
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

/** Executor 只做两种 bound statement 的封闭分派与公开结果组装。 */
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
                new BoundClusteredInsert(table, List.of(new SqlValue.IntegerValue(BigInteger.ONE))), status,
                SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals(1, update.affectedRows());

        QueryResult query = assertInstanceOf(QueryResult.class, executor.execute(handle,
                new BoundPointSelect(table, List.of(0), 3, PointAccessKind.CLUSTERED_PRIMARY,
                        List.of(new SqlValue.IntegerValue(BigInteger.ONE))), status,
                SqlStatementDeadline.after(Duration.ofSeconds(1))));
        assertEquals("id", query.columns().getFirst().name());
        assertEquals(BigInteger.ONE, assertInstanceOf(SqlValue.IntegerValue.class,
                query.rows().getFirst().values().getFirst()).value());
        assertEquals(2, gateway.calls);
    }

    private static TableDefinition table() {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        SpaceId space = SpaceId.of(5);
        TableStorageBinding binding = new TableStorageBinding(2, space, Path.of("docs.ibd"), List.of(
                new IndexStorageBinding(3, PageId.of(space, PageNo.of(10)), 0,
                        new SegmentRef(space, 1, SegmentId.of(1)), new SegmentRef(space, 2, SegmentId.of(2)))),
                Optional.empty());
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("docs"),
                DictionaryVersion.of(2), TableState.ACTIVE, List.of(id), List.of(primary), Optional.of(binding));
    }

    private static final class TestHandle implements SqlTransactionHandle { }

    private static final class RecordingGateway implements SqlStorageGateway {
        private int calls;
        @Override public SqlTransactionHandle begin(SqlTransactionRequest request) { return new TestHandle(); }
        @Override public SqlWriteOutcome insert(SqlTransactionHandle transaction, BoundClusteredInsert statement,
                                                SqlStatementDeadline deadline) {
            calls++; return new SqlWriteOutcome(1, false);
        }
        @Override public Optional<SqlRow> selectPoint(SqlTransactionHandle transaction,
                                                     BoundPointSelect statement,
                                                     SqlStatementDeadline deadline) {
            calls++; return Optional.of(new SqlRow(List.of(new SqlValue.IntegerValue(BigInteger.ONE))));
        }
        @Override public SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request) {
            return new SqlCommitOutcome(0, true, 0);
        }
        @Override public SqlRollbackOutcome rollback(SqlTransactionHandle transaction) {
            return new SqlRollbackOutcome(0, 0);
        }
    }
}
