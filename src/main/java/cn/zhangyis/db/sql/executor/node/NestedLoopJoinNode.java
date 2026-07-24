package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.expression.ExpressionEvaluator;
import cn.zhangyis.db.sql.executor.row.JoinedSqlRowView;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * 左驱动 Nested Loop Join。outer 只打开一次；每个非 NULL outer key 通过工厂创建并打开
 * 一个新的 inner subtree。工厂可产生全扫或索引 probe，JoinNode 不读取索引/存储内部类型。
 */
public final class NestedLoopJoinNode
        extends AbstractPlanNode {
    /** SQL 左输入，生命周期覆盖全部 inner probe。 */
    private final PlanNode outer;
    /** outer key 到一次性右输入节点的无状态工厂。 */
    private final Function<SqlValue, PlanNode> innerFactory;
    /** outer relation 中提供 ON probe key 的 local ordinal。 */
    private final int outerKeyOrdinal;
    /** 最终 ON 三值条件；只有 TRUE 发布 joined row。 */
    private final PredicateSet joinPredicates;
    /** 与 Filter 共用的无状态三值解释器。 */
    private final ExpressionEvaluator evaluator;
    /** 左列后拼接右列的扁平输出 schema。 */
    private final List<ResultColumn> columns;
    /** open 时保存的本语句上下文；所有 inner 必须复用其 scope/deadline。 */
    private ExecutionContext context;
    /** 当前 outer row 对应的右输入；EOF 后立即关闭并清空。 */
    private PlanNode inner;

    /**
     * @param outer SQL 左输入节点
     * @param innerFactory 每次 outer row 创建 NEW 右输入节点的工厂
     * @param outerKeyOrdinal 左行 ON 列 local ordinal
     * @param joinPredicates 完整 ON 条件
     * @param evaluator SQL 三值解释器
     * @param innerColumns 右表完整输出 schema
     */
    public NestedLoopJoinNode(
            PlanNode outer,
            Function<SqlValue, PlanNode> innerFactory,
            int outerKeyOrdinal,
            PredicateSet joinPredicates,
            ExpressionEvaluator evaluator,
            List<ResultColumn> innerColumns) {
        if (outer == null || innerFactory == null
                || outerKeyOrdinal < 0
                || outerKeyOrdinal >= outer.columns().size()
                || joinPredicates == null || evaluator == null
                || innerColumns == null || innerColumns.isEmpty()
                || innerColumns.stream()
                .anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "invalid Nested Loop Join node fields");
        }
        this.outer = outer;
        this.innerFactory = innerFactory;
        this.outerKeyOrdinal = outerKeyOrdinal;
        this.joinPredicates = joinPredicates;
        this.evaluator = evaluator;
        ArrayList<ResultColumn> joined =
                new ArrayList<>(outer.columns());
        joined.addAll(innerColumns);
        this.columns = List.copyOf(joined);
    }

    /**
     * 只打开 outer；inner 必须等首个 outer key 出现后才创建，避免空左表仍创建右 cursor/ReadView。
     *
     * @param context 持有同一 transaction、deadline 与 statement cursor scope 的上下文
     */
    @Override
    protected void openNode(
            ExecutionContext context) {
        this.context = context;
        outer.open(context);
    }

    /**
     * 拉取下一条 ON 为 TRUE 的连接行。
     *
     * <ol>
     *     <li>优先继续当前 inner；每行构造零拷贝 joined view 并执行完整 ON，FALSE/UNKNOWN 继续。</li>
     *     <li>inner EOF 后立即关闭，释放其局部 cursor，再推进 outer。</li>
     *     <li>outer key 为 NULL 时按 SQL 等值 UNKNOWN 跳过，不打开无结果 inner。</li>
     *     <li>用非 NULL key 创建 NEW inner 并复用同一 ExecutionContext 打开，然后回到阶段 1。</li>
     * </ol>
     *
     * @return 下一条匹配 joined view；outer EOF 时为 Java null
     */
    @Override
    protected SqlRowView advanceNode() {
        while (true) {
            // 1、inner 前进前上一次 joined view 已被 AbstractPlanNode 清除；child generation 随后使其失效。
            if (inner != null) {
                while (inner.advance()) {
                    JoinedSqlRowView joined =
                            new JoinedSqlRowView(
                                    outer.current(),
                                    inner.current());
                    if (evaluator.matches(
                            joinPredicates.condition(),
                            joined)) {
                        return joined;
                    }
                }
            }
            // 2、每个 outer row 的 inner subtree 都必须在推进 outer 前收口。
            closeInner();
            if (!outer.advance()) {
                return null;
            }
            // 3、NULL = anything 只能得到 UNKNOWN，不能产生 INNER JOIN 行。
            SqlRowView outerRow =
                    outer.current();
            if (outerRow.isNullAt(
                    outerKeyOrdinal)) {
                continue;
            }
            SqlValue key = outerRow.valueAt(
                    outerKeyOrdinal);
            // 4、右输入节点不可复用，必须为 NEW 并共享 statement cursor scope。
            inner = innerFactory.apply(key);
            if (inner == null) {
                throw new DatabaseValidationException(
                        "INNER JOIN probe factory returned null");
            }
            inner.open(context);
        }
    }

    /**
     * 逆序关闭当前 inner 和 outer；任一关闭失败不能跳过另一资源。
     */
    @Override
    protected void closeNode() {
        RuntimeException failure = null;
        try {
            closeInner();
        } catch (RuntimeException innerFailure) {
            failure = innerFailure;
        }
        try {
            outer.close();
        } catch (RuntimeException outerFailure) {
            if (failure == null) {
                failure = outerFailure;
            } else {
                failure.addSuppressed(
                        outerFailure);
            }
        }
        context = null;
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public List<ResultColumn> columns() {
        return columns;
    }

    private void closeInner() {
        if (inner != null) {
            PlanNode closing = inner;
            inner = null;
            closing.close();
        }
    }
}
