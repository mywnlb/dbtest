package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;

import java.util.ArrayList;

/**
 * 递归展平 AND，同时保持用户谓词顺序；不做排序或去重。
 */
public final class FlattenConjunctionRule implements OptimizerRule {
    @Override
    public String name() {
        return "flatten-conjunction";
    }

    @Override
    public RuleResult apply(LogicalPlan plan) {
        LogicalPlan rewritten =
                LogicalExpressionRewriter.rewrite(plan, this::flatten);
        return rewritten == plan
                ? RuleResult.unchanged(plan)
                : RuleResult.transformed(rewritten);
    }

    private BoundExpression flatten(BoundExpression expression) {
        if (!(expression instanceof BoundConjunction conjunction)) {
            return expression;
        }
        ArrayList<BoundExpression> flattened = new ArrayList<>();
        boolean changed = false;
        for (BoundExpression operand : conjunction.operands()) {
            if (operand instanceof BoundConjunction nested) {
                flattened.addAll(nested.operands());
                changed = true;
            } else {
                flattened.add(operand);
            }
        }
        return changed ? new BoundConjunction(flattened) : conjunction;
    }
}
