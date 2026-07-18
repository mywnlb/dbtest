package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.BoundSecondaryRangeSelect;
import cn.zhangyis.db.sql.binder.bound.BoundStatement;
import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
import cn.zhangyis.db.sql.executor.storage.SqlStorageGateway;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;

import java.util.List;

/** 两种确定性 bound plan 的薄执行器；事务控制和 autocommit 状态机属于 Session。 */
public final class DefaultSqlExecutor {
    private final SqlStorageGateway storage;

    public DefaultSqlExecutor(SqlStorageGateway storage) {
        if (storage == null) throw new DatabaseValidationException("SQL storage gateway must not be null");
        this.storage = storage;
    }

    /** exhaustive 分派 INSERT/primary-point SELECT，并用 exact DD projection 组装公开结果。 */
    public SqlExecutionResult execute(SqlTransactionHandle transaction, BoundStatement statement,
                                       TransactionStatus status, SqlStatementDeadline deadline) {
        if (transaction == null || statement == null || status == null || deadline == null) {
            throw new DatabaseValidationException("executor transaction/statement/status/deadline must not be null");
        }
        return switch (statement) {
            case BoundClusteredInsert insert -> {
                var outcome = storage.insert(transaction, insert, deadline);
                yield new UpdateResult(outcome.affectedRows(),
                        new TransactionStatus(status.autocommit(), status.transactionActive(),
                                outcome.rollbackOnly()));
            }
            case BoundPointSelect select -> {
                List<ResultColumn> columns = resultColumns(select.table(), select.projectionOrdinals());
                List<SqlRow> rows = storage.selectPoint(transaction, select, deadline).stream().toList();
                yield new QueryResult(columns, rows, status);
            }
            case BoundSecondaryRangeSelect select -> new QueryResult(
                    resultColumns(select.table(), select.projectionOrdinals()),
                    storage.selectRange(transaction, select, deadline), status);
            case BoundUpdate update -> {
                var outcome = storage.update(transaction, update, deadline);
                yield new UpdateResult(outcome.affectedRows(),
                        new TransactionStatus(status.autocommit(), status.transactionActive(),
                                outcome.rollbackOnly()));
            }
            case BoundDelete delete -> {
                var outcome = storage.delete(transaction, delete, deadline);
                yield new UpdateResult(outcome.affectedRows(),
                        new TransactionStatus(status.autocommit(), status.transactionActive(),
                                outcome.rollbackOnly()));
            }
            case BoundCreateIndex ignored -> throw new DatabaseValidationException(
                    "bound CREATE INDEX must be executed by Session DDL coordinator");
        };
    }

    /** 按 Binder 已验证的 ordinal 构造公开列元数据，point/range SELECT 共用同一 exact DD snapshot。 */
    private static List<ResultColumn> resultColumns(
            cn.zhangyis.db.dd.domain.TableDefinition table, List<Integer> ordinals) {
        return ordinals.stream().map(ordinal -> {
            var column = table.columns().get(ordinal);
            return new ResultColumn(column.name().displayName(), column.type());
        }).toList();
    }
}
