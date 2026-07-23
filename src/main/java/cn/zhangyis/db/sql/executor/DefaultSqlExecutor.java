package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.storage.SqlDataAccessPort;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStorageException;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPlan;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalInsert;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointSelect;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeSelect;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSecondaryRangeSelect;

import java.util.List;

/**
 * M1 物理计划执行器；事务控制和 autocommit 状态机属于 Session，存储内部资源由 Data Port 管理。
 */
public final class DefaultSqlExecutor implements SqlExecutor {
    /**
     * 本对象持有的 {@code storage} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SqlDataAccessPort storage;

    /**
     * 创建 {@code DefaultSqlExecutor}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param storage 由组合根提供的最小数据访问端口；不得为 {@code null}，生命周期必须覆盖执行器
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DefaultSqlExecutor(SqlDataAccessPort storage) {
        if (storage == null) {
            throw new DatabaseValidationException("SQL data access port must not be null");
        }
        this.storage = storage;
    }

    /**
     * exhaustive 分派物理计划，并用 exact DD projection 组装公开结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务能力、物理计划、事务状态和共享 deadline，缺失输入早于 Data Port 调用失败。</li>
     *     <li>按 sealed PhysicalPlan 种类选择唯一 Data Port 方法，Executor 不重新选择访问索引。</li>
     *     <li>查询分支按 exact DD ordinal 构造列元数据并接收完整行集；写分支合并 affected rows 与
     *         storage rollback-only 状态。</li>
     *     <li>返回不可变公开结果；Data Port 异常原样传播，查询不会组装 partial result。</li>
     * </ol>
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param plan Compiler 产生的不可变物理计划；必须属于当前 metadata lease
     * @param status 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code execute} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws SqlStorageException Data Port 无法完成完整访问时抛出；不会组装或返回部分查询结果
     */
    @Override
    public SqlExecutionResult execute(
            SqlTransactionHandle transaction, PhysicalPlan plan,
            TransactionStatus status, SqlStatementDeadline deadline) {
        // 1、缺失能力或计划必须在下游创建事务锁、ReadView 或 statement guard 前失败。
        if (transaction == null || plan == null || status == null || deadline == null) {
            throw new DatabaseValidationException(
                    "executor transaction/plan/status/deadline must not be null");
        }
        // 2、sealed switch 保证新增 PhysicalPlan 必须显式决定唯一执行端口。
        // 3、每个分支只做公开结果适配；事务、ReadView、锁和 statement guard 都归 Data Port。
        SqlExecutionResult result = switch (plan) {
            case PhysicalInsert insert -> {
                var outcome = storage.insert(transaction, insert, deadline);
                yield new UpdateResult(outcome.affectedRows(),
                        new TransactionStatus(status.autocommit(), status.transactionActive(),
                                outcome.rollbackOnly()));
            }
            case PhysicalPointSelect select -> {
                List<ResultColumn> columns = resultColumns(select.table(), select.projectionOrdinals());
                List<SqlRow> rows = storage.selectPoint(transaction, select, deadline).stream().toList();
                yield new QueryResult(columns, rows, status);
            }
            case PhysicalSecondaryRangeSelect select -> new QueryResult(
                    resultColumns(select.table(), select.projectionOrdinals()),
                    storage.selectRange(transaction, select, deadline), status);
            case PhysicalRangeSelect select -> new QueryResult(
                    resultColumns(select.table(), select.projectionOrdinals()),
                    storage.selectRange(transaction, select, deadline), status);
            case PhysicalPointUpdate update -> {
                var outcome = storage.update(transaction, update, deadline);
                yield new UpdateResult(outcome.affectedRows(),
                        new TransactionStatus(status.autocommit(), status.transactionActive(),
                                outcome.rollbackOnly()));
            }
            case PhysicalPointDelete delete -> {
                var outcome = storage.delete(transaction, delete, deadline);
                yield new UpdateResult(outcome.affectedRows(),
                        new TransactionStatus(status.autocommit(), status.transactionActive(),
                                outcome.rollbackOnly()));
            }
            case PhysicalRangeUpdate update -> {
                var outcome = storage.updateRange(transaction, update, deadline);
                yield new UpdateResult(outcome.affectedRows(),
                        new TransactionStatus(status.autocommit(), status.transactionActive(),
                                outcome.rollbackOnly()));
            }
            case PhysicalRangeDelete delete -> {
                var outcome = storage.deleteRange(transaction, delete, deadline);
                yield new UpdateResult(outcome.affectedRows(),
                        new TransactionStatus(status.autocommit(), status.transactionActive(),
                                outcome.rollbackOnly()));
            }
        };
        // 4、只有 Data Port 完整返回后才发布本次结果对象。
        return result;
    }

    /**
     * 按 Compiler 已验证的 ordinal 构造公开列元数据，point/range SELECT 共用同一 exact DD snapshot。
     *
     * @param table physical plan 绑定且由 metadata lease 保护的 exact table version
     * @param ordinals 保持用户投影顺序的列 ordinal
     * @return 与公开 SqlRow 列顺序一一对应的不可变结果列描述
     */
    private static List<ResultColumn> resultColumns(
            cn.zhangyis.db.dd.domain.TableDefinition table, List<Integer> ordinals) {
        return ordinals.stream().map(ordinal -> {
            var column = table.columns().get(ordinal);
            return new ResultColumn(column.name().displayName(), column.type());
        }).toList();
    }
}
