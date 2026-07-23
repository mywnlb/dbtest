package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.optimizer.logical.LogicalFilter;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;
import cn.zhangyis.db.sql.optimizer.logical.LogicalProject;
import cn.zhangyis.db.sql.optimizer.logical.LogicalTableModify;
import cn.zhangyis.db.sql.optimizer.logical.LogicalTableScan;
import cn.zhangyis.db.sql.optimizer.logical.LogicalValues;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.optimizer.logical.RelNode;

import java.util.function.UnaryOperator;

/**
 * 对当前封闭关系树中的 LogicalFilter condition 做不可变重建。
 */
final class LogicalExpressionRewriter {
    private LogicalExpressionRewriter() {
    }

    static LogicalPlan rewrite(
            LogicalPlan plan, UnaryOperator<BoundExpression> expressionRule) {
        RelNode rewritten = rewriteNode(plan.root(), expressionRule);
        return rewritten.equals(plan.root()) ? plan : new LogicalPlan(rewritten);
    }

    private static RelNode rewriteNode(
            RelNode node, UnaryOperator<BoundExpression> expressionRule) {
        return switch (node) {
            case LogicalFilter filter -> {
                RelNode input = rewriteNode(filter.input(), expressionRule);
                BoundExpression condition =
                        BoundExpressionTreeRewriter.rewrite(
                                filter.predicates().condition(),
                                expressionRule);
                if (input.equals(filter.input())
                        && condition.equals(filter.predicates().condition())) {
                    yield filter;
                }
                yield new LogicalFilter(input, PredicateSet.of(condition));
            }
            case LogicalProject project -> {
                RelNode input = rewriteNode(project.input(), expressionRule);
                yield input.equals(project.input()) ? project
                        : new LogicalProject(input, project.projectionOrdinals());
            }
            case LogicalTableModify modify -> {
                RelNode input = rewriteNode(modify.input(), expressionRule);
                yield input.equals(modify.input()) ? modify
                        : new LogicalTableModify(
                        modify.table(), modify.kind(), input,
                        modify.assignmentOrdinals(), modify.assignmentValues());
            }
            case LogicalTableScan scan -> scan;
            case LogicalValues values -> values;
        };
    }
}
