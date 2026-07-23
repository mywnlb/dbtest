package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;

import java.util.ArrayList;

/**
 * 递归展平 OR，同时保持用户分支顺序；不实施 range union、排序或去重。
 */
public final class FlattenDisjunctionRule implements OptimizerRule {
    /**
     * 返回固定规则身份，供 RuleProgram 检查重复名称和稳定执行顺序。
     *
     * @return 非空且稳定的规则名
     */
    @Override
    public String name() {
        return "flatten-disjunction";
    }

    /**
     * 对逻辑计划中的每个 filter condition 执行 bottom-up OR 展平。
     *
     * @param plan 当前完整不可变逻辑计划
     * @return 未变化时携带原计划的结果，变化时携带完整重建计划
     */
    @Override
    public RuleResult apply(LogicalPlan plan) {
        LogicalPlan rewritten =
                LogicalExpressionRewriter.rewrite(plan, this::flatten);
        return rewritten == plan
                ? RuleResult.unchanged(plan)
                : RuleResult.transformed(rewritten);
    }

    /**
     * 展平当前 OR 节点的直接嵌套分支；child 已由统一 rewriter 先行处理。
     *
     * @param expression bottom-up 改写后的当前节点
     * @return 无嵌套 OR 的节点，或原引用
     */
    private BoundExpression flatten(BoundExpression expression) {
        if (!(expression instanceof BoundDisjunction disjunction)) {
            return expression;
        }
        ArrayList<BoundExpression> flattened = new ArrayList<>();
        boolean changed = false;
        for (BoundExpression operand : disjunction.operands()) {
            if (operand instanceof BoundDisjunction nested) {
                flattened.addAll(nested.operands());
                changed = true;
            } else {
                flattened.add(operand);
            }
        }
        return changed
                ? new BoundDisjunction(flattened)
                : disjunction;
    }
}
