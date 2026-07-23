package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;

/**
 * 把 literal-column comparison 规范为 column-literal，并反转操作符方向。
 */
public final class CanonicalizeComparisonRule implements OptimizerRule {
    @Override
    public String name() {
        return "canonicalize-comparison";
    }

    @Override
    public RuleResult apply(LogicalPlan plan) {
        LogicalPlan rewritten =
                LogicalExpressionRewriter.rewrite(plan, this::canonicalize);
        return rewritten == plan
                ? RuleResult.unchanged(plan)
                : RuleResult.transformed(rewritten);
    }

    private BoundExpression canonicalize(BoundExpression expression) {
        if (expression instanceof BoundComparison comparison
                && comparison.left() instanceof BoundLiteral
                && comparison.right() instanceof BoundColumnReference) {
            return new BoundComparison(
                    comparison.right(), comparison.operator().reversed(),
                    comparison.left());
        }
        return expression;
    }
}
