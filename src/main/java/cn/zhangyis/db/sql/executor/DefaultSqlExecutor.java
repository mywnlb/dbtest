package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPrimaryPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundStatement;
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
            case BoundPrimaryPointSelect select -> {
                List<ResultColumn> columns = select.projectionOrdinals().stream().map(ordinal -> {
                    var column = select.table().columns().get(ordinal);
                    return new ResultColumn(column.name().displayName(), column.type());
                }).toList();
                List<SqlRow> rows = storage.selectPoint(transaction, select, deadline).stream().toList();
                yield new QueryResult(columns, rows, status);
            }
        };
    }
}
