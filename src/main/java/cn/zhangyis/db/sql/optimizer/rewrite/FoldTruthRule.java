package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.expression.BoundNegation;
import cn.zhangyis.db.sql.expression.BoundNullTest;
import cn.zhangyis.db.sql.expression.BoundNullTestOperator;
import cn.zhangyis.db.sql.expression.BoundTruthLiteral;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;
import cn.zhangyis.db.sql.type.SqlBoolean;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.ArrayList;

/**
 * 仅执行不依赖 collation、函数副作用或访问路径的 SQL 三值常量折叠。
 */
public final class FoldTruthRule implements OptimizerRule {
    @Override
    public String name() {
        return "fold-sql-truth";
    }

    @Override
    public RuleResult apply(LogicalPlan plan) {
        LogicalPlan rewritten =
                LogicalExpressionRewriter.rewrite(plan, this::fold);
        return rewritten == plan
                ? RuleResult.unchanged(plan)
                : RuleResult.transformed(rewritten);
    }

    private BoundExpression fold(BoundExpression expression) {
        if (expression instanceof BoundComparison comparison
                && (isNullLiteral(comparison.left())
                || isNullLiteral(comparison.right()))) {
            return new BoundTruthLiteral(
                    SqlBoolean.UNKNOWN, comparison.position());
        }
        if (expression instanceof BoundNullTest nullTest
                && nullTest.operand() instanceof BoundLiteral literal) {
            boolean isNull =
                    literal.value() instanceof SqlValue.NullValue;
            boolean result =
                    nullTest.operator() == BoundNullTestOperator.IS_NULL
                            ? isNull : !isNull;
            return new BoundTruthLiteral(
                    result ? SqlBoolean.TRUE : SqlBoolean.FALSE,
                    nullTest.position());
        }
        if (expression instanceof BoundNegation negation
                && negation.operand()
                instanceof BoundTruthLiteral truth) {
            return new BoundTruthLiteral(
                    truth.value().not(), negation.position());
        }
        if (expression instanceof BoundConjunction conjunction) {
            return foldConjunction(conjunction);
        }
        if (expression instanceof BoundDisjunction disjunction) {
            return foldDisjunction(disjunction);
        }
        return expression;
    }

    /**
     * 折叠当前 AND 的 truth operand；child 已由 bottom-up rewriter 处理。
     *
     * @param conjunction 当前 conjunction
     * @return SQL 三值等价的最小表达式
     */
    private BoundExpression foldConjunction(
            BoundConjunction conjunction) {
        ArrayList<BoundExpression> operands = new ArrayList<>();
        boolean changed = false;
        for (BoundExpression operand : conjunction.operands()) {
            if (operand instanceof BoundTruthLiteral truth) {
                if (truth.value() == SqlBoolean.FALSE) {
                    return new BoundTruthLiteral(
                            SqlBoolean.FALSE, conjunction.position());
                }
                if (truth.value() == SqlBoolean.TRUE) {
                    changed = true;
                    continue;
                }
            }
            operands.add(operand);
        }
        if (operands.isEmpty()) {
            return new BoundTruthLiteral(
                    SqlBoolean.TRUE, conjunction.position());
        }
        if (operands.size() == 1) {
            return operands.getFirst();
        }
        if (operands.stream().allMatch(BoundTruthLiteral.class::isInstance)) {
            SqlBoolean result = SqlBoolean.TRUE;
            for (BoundExpression operand : operands) {
                result = result.and(((BoundTruthLiteral) operand).value());
            }
            return new BoundTruthLiteral(result, conjunction.position());
        }
        return changed ? new BoundConjunction(operands) : conjunction;
    }

    /**
     * 折叠当前 OR 的 truth operand；TRUE 短路，FALSE 可安全删除，UNKNOWN 必须保留。
     *
     * @param disjunction 当前 disjunction
     * @return SQL 三值等价的最小表达式
     */
    private BoundExpression foldDisjunction(
            BoundDisjunction disjunction) {
        ArrayList<BoundExpression> operands = new ArrayList<>();
        boolean changed = false;
        for (BoundExpression operand : disjunction.operands()) {
            if (operand instanceof BoundTruthLiteral truth) {
                if (truth.value() == SqlBoolean.TRUE) {
                    return new BoundTruthLiteral(
                            SqlBoolean.TRUE, disjunction.position());
                }
                if (truth.value() == SqlBoolean.FALSE) {
                    changed = true;
                    continue;
                }
            }
            operands.add(operand);
        }
        if (operands.isEmpty()) {
            return new BoundTruthLiteral(
                    SqlBoolean.FALSE, disjunction.position());
        }
        if (operands.size() == 1) {
            return operands.getFirst();
        }
        if (operands.stream().allMatch(
                BoundTruthLiteral.class::isInstance)) {
            SqlBoolean result = SqlBoolean.FALSE;
            for (BoundExpression operand : operands) {
                result = result.or(
                        ((BoundTruthLiteral) operand).value());
            }
            return new BoundTruthLiteral(
                    result, disjunction.position());
        }
        return changed
                ? new BoundDisjunction(operands)
                : disjunction;
    }

    private static boolean isNullLiteral(BoundExpression expression) {
        return expression instanceof BoundLiteral literal
                && literal.value() instanceof SqlValue.NullValue;
    }
}
