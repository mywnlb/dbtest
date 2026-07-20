package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.BoundSecondaryRangeSelect;
import cn.zhangyis.db.sql.binder.bound.BoundStatement;
import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
import cn.zhangyis.db.sql.binder.bound.BoundDropIndex;
import cn.zhangyis.db.sql.executor.storage.SqlStorageGateway;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;

import java.util.List;

/** 两种确定性 bound plan 的薄执行器；事务控制和 autocommit 状态机属于 Session。 */
public final class DefaultSqlExecutor {
    /**
     * 本对象持有的 {@code storage} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SqlStorageGateway storage;

    /**
     * 创建 {@code DefaultSqlExecutor}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param storage 由组合根提供的 {@code SqlStorageGateway} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DefaultSqlExecutor(SqlStorageGateway storage) {
        if (storage == null) throw new DatabaseValidationException("SQL storage gateway must not be null");
        this.storage = storage;
    }

    /** exhaustive 分派 INSERT/primary-point SELECT，并用 exact DD projection 组装公开结果。
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param statement 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param status 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code execute} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
            case BoundDropIndex ignored -> throw new DatabaseValidationException(
                    "bound DROP INDEX must be executed by Session DDL coordinator");
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
