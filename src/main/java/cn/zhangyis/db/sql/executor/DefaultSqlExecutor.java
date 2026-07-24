package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.exception.SqlExecutionException;
import cn.zhangyis.db.sql.executor.expression.ExpressionEvaluator;
import cn.zhangyis.db.sql.executor.node.PlanNode;
import cn.zhangyis.db.sql.executor.node.PlanNodeFactory;
import cn.zhangyis.db.sql.executor.node.SortExecutionConfig;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
import cn.zhangyis.db.sql.executor.storage.SqlDataAccessPort;
import cn.zhangyis.db.sql.executor.storage.SqlCursorScope;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStorageException;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPlan;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalInsert;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalJoinQuery;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalQuery;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeUpdate;

import java.util.ArrayList;
import java.util.List;

/**
 * M4 物理计划执行器；SELECT 创建 statement-private pull tree，公开结果仍 eager。
 * 事务控制和 autocommit 状态机属于 Session，存储内部资源由 Data Port cursor 管理。
 */
public final class DefaultSqlExecutor implements SqlExecutor {
    /** 单条查询最多发布的结果行数；多取一行只用于 fail-closed 检测。 */
    private static final int QUERY_RESULT_ROW_LIMIT = 4096;

    /**
     * 本对象持有的 {@code storage} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SqlDataAccessPort storage;
    /** 不可变 PhysicalQuery 到每语句 PlanNode tree 的 builder。 */
    private final PlanNodeFactory planNodeFactory;

    /**
     * 创建 {@code DefaultSqlExecutor}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param storage 由组合根提供的最小数据访问端口；不得为 {@code null}，生命周期必须覆盖执行器
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DefaultSqlExecutor(SqlDataAccessPort storage) {
        this(storage, SortExecutionConfig.defaults());
    }

    /**
     * 创建使用实例级排序目录与显式资源预算的 SQL 执行器。
     *
     * @param storage 由组合根提供的最小数据访问端口；不得为 {@code null}
     * @param sortConfig 实例受控临时根、内存/文件预算与归并 fan-in；不得为 {@code null}
     * @throws DatabaseValidationException 任一协作者缺失时抛出，且不创建临时文件或 storage cursor
     */
    public DefaultSqlExecutor(
            SqlDataAccessPort storage,
            SortExecutionConfig sortConfig) {
        if (storage == null) {
            throw new DatabaseValidationException("SQL data access port must not be null");
        }
        if (sortConfig == null) {
            throw new DatabaseValidationException(
                    "SQL sort execution config must not be null");
        }
        this.storage = storage;
        this.planNodeFactory = new PlanNodeFactory(
                storage, new ExpressionEvaluator(), sortConfig);
    }

    /**
     * exhaustive 分派物理计划，并在 SELECT/DML 各自资源边界组装公开结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务能力、物理计划、事务状态和共享 deadline，缺失输入早于 Data Port 调用失败。</li>
     *     <li>按 sealed PhysicalPlan 种类选择查询节点树或唯一 DML Data Port 方法，不重新选索引。</li>
     *     <li>查询分支拉取 Project/Filter/Access 并在结果边界物化；写分支合并 affected rows 与
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
        // 2、sealed switch 保证新增 PhysicalPlan 必须显式决定节点树或 DML 执行端口。
        // 3、查询运行状态归 statement-private PlanNode；ReadView/锁/statement guard 仍归 Data Port。
        SqlExecutionResult result = switch (plan) {
            case PhysicalInsert insert -> {
                var outcome = storage.insert(transaction, insert, deadline);
                yield new UpdateResult(outcome.affectedRows(),
                        new TransactionStatus(status.autocommit(), status.transactionActive(),
                                outcome.rollbackOnly()),
                        outcome.firstGeneratedKey());
            }
            case PhysicalQuery query ->
                    executeQuery(
                            transaction,
                            planNodeFactory.create(query),
                            status, deadline);
            case PhysicalJoinQuery query ->
                    executeQuery(
                            transaction,
                            planNodeFactory.create(query),
                            status, deadline);
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
     * 打开并拉取整棵 SELECT PlanNode tree，在公开 eager QueryResult 边界才物化全部行。
     *
     * <ol>
     *     <li>由工厂创建 statement-private 节点树并 child-first 打开；失败时模板收敛部分资源。</li>
     *     <li>逐行 advance，立即把 cursor-owned row view 复制为 SqlRow，旧视图不跨下一次 advance。</li>
     *     <li>Filter 后结果超过 4096 时 fail-closed，不发布已物化前缀。</li>
     *     <li>finally 逆序关闭根节点；RC ReadView/cursor close 失败保留主异常或直接阻止结果发布。</li>
     * </ol>
     *
     * @param transaction 当前 Session 持有的不透明事务能力
     * @param query Optimizer 产生的 project(filter(access)) 根及不可变 order/limit/strategy 属性
     * @param status 本语句完成前的事务状态快照
     * @param deadline parse/bind/optimize/execute 共用的绝对期限
     * @return 完整、不可变且不含 cursor/storage reference 的查询结果
     * @throws SqlExecutionException 节点协议、结果容量或 row width 无效时抛出
     * @throws SqlStorageException cursor/MVCC/锁/LOB 失败时抛出
     */
    private QueryResult executeQuery(
            SqlTransactionHandle transaction, PlanNode root,
            TransactionStatus status, SqlStatementDeadline deadline) {
        // 1、PhysicalPlan 本身无运行期状态；调用分支已经为本次执行创建唯一节点树。
        ArrayList<SqlRow> rows = new ArrayList<>();
        RuntimeException failure = null;
        SqlCursorScope cursorScope = null;
        try {
            cursorScope = storage.openCursorScope(
                    transaction, deadline);
            if (cursorScope == null) {
                throw new DatabaseValidationException(
                        "SQL data port returned null cursor scope");
            }
            root.open(new ExecutionContext(
                    transaction, deadline,
                    java.util.Optional.of(
                            cursorScope)));
            // 2、当前 row view 必须在下一次 advance 前完全复制，LOB hydration 也在该窗口完成。
            while (root.advance()) {
                var view = root.current();
                ArrayList<cn.zhangyis.db.sql.type.SqlValue> values =
                        new ArrayList<>(view.width());
                for (int ordinal = 0; ordinal < view.width(); ordinal++) {
                    values.add(view.valueAt(ordinal));
                }
                rows.add(new SqlRow(values));
                // 3、公开结果仍是 eager list，因此输出容量必须在发布 QueryResult 前有界。
                if (rows.size() > QUERY_RESULT_ROW_LIMIT) {
                    throw new SqlExecutionException(
                            "query result row limit exceeded: "
                                    + QUERY_RESULT_ROW_LIMIT);
                }
            }
        } catch (RuntimeException executionFailure) {
            failure = executionFailure;
            throw executionFailure;
        } finally {
            // 4、先逆序关闭节点局部 cursor，再由 scope 统一释放 RC view/operation lease。
            RuntimeException cleanupFailure = null;
            try {
                root.close();
            } catch (RuntimeException closeFailure) {
                cleanupFailure = closeFailure;
            }
            if (cursorScope != null) {
                try {
                    cursorScope.close();
                } catch (RuntimeException closeFailure) {
                    if (cleanupFailure == null) {
                        cleanupFailure = closeFailure;
                    } else {
                        cleanupFailure.addSuppressed(
                                closeFailure);
                    }
                }
            }
            if (cleanupFailure != null) {
                if (failure == null) {
                    throw cleanupFailure;
                }
                failure.addSuppressed(
                        cleanupFailure);
            }
        }
        return new QueryResult(root.columns(), List.copyOf(rows), status);
    }
}
