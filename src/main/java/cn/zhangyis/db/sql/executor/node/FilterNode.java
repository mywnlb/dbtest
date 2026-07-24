package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.expression.ExpressionEvaluator;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;

import java.util.List;

/**
 * 在 child 完整逻辑行上执行最终 SQL truth 的 pull Filter。
 */
public final class FilterNode extends AbstractPlanNode {
    /** 唯一 child；先 open、后 close。 */
    private final PlanNode input;
    /** canonical residual；访问范围不能替代它。 */
    private final PredicateSet predicates;
    /** 无状态三值解释器。 */
    private final ExpressionEvaluator evaluator;

    /**
     * @param input storage access child
     * @param predicates 完整 canonical residual
     * @param evaluator 共享表达式解释器
     * @throws DatabaseValidationException 任一参数缺失时抛出
     */
    public FilterNode(
            PlanNode input, PredicateSet predicates,
            ExpressionEvaluator evaluator) {
        if (input == null || predicates == null || evaluator == null) {
            throw new DatabaseValidationException(
                    "filter node collaborators must not be null");
        }
        this.input = input;
        this.predicates = predicates;
        this.evaluator = evaluator;
    }

    /**
     * 先打开 access child；本节点不创建额外存储资源。
     *
     * @param context 当前语句运行上下文
     */
    @Override
    protected void openNode(ExecutionContext context) {
        input.open(context);
    }

    /**
     * 持续拉取候选并在完整行上执行最终三值 residual。
     *
     * <ol>
     *     <li>从 child 拉取一个已完成 MVCC/current-read 的候选行。</li>
     *     <li>仅 evaluator 返回 SQL TRUE 时发布；FALSE/UNKNOWN 继续拉取直到 EOF。</li>
     * </ol>
     *
     * @return 第一条满足最终 SQL truth 的行，child EOF 时为 Java {@code null}
     */
    @Override
    protected SqlRowView advanceNode() {
        // 1、每轮 input.advance 都会使上一候选视图失效。
        while (input.advance()) {
            SqlRowView candidate = input.current();
            // 2、access range 不能替代完整 residual，UNKNOWN 与 FALSE 都不发布。
            if (evaluator.matches(predicates.condition(), candidate)) {
                return candidate;
            }
        }
        return null;
    }

    /** 逆序关闭唯一 child；表达式解释器无资源。 */
    @Override
    protected void closeNode() {
        input.close();
    }

    @Override
    public List<ResultColumn> columns() {
        return input.columns();
    }
}
