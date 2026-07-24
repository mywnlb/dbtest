package cn.zhangyis.db.sql.executor.expression;

import cn.zhangyis.db.sql.executor.exception.SqlExecutionException;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.expression.BoundNegation;
import cn.zhangyis.db.sql.expression.BoundNullTest;
import cn.zhangyis.db.sql.expression.BoundNullTestOperator;
import cn.zhangyis.db.sql.expression.BoundTruthLiteral;
import cn.zhangyis.db.sql.type.SqlBoolean;
import cn.zhangyis.db.sql.type.SqlValue;

/**
 * M4 canonical BoundExpression 解释器。它只理解 SQL 三值组合和 row-view 端口，
 * 不读取 DD repository、B+Tree、record、ReadView 或其它存储内部状态。
 */
public final class ExpressionEvaluator {

    /**
     * 在当前逻辑行上递归求值完整 boolean residual。
     *
     * <ol>
     *     <li>按 sealed expression 穷尽分派，truth literal 直接返回三值常量。</li>
     *     <li>AND/OR/NOT 使用 {@link SqlBoolean} 并保持短路，避免无必要 LOB/value 读取。</li>
     *     <li>NULL test 只调用 row 的 NULL 探针；comparison 使用 row 的 exact comparator。</li>
     *     <li>scalar 根或未规范 comparison 显式失败，不把非法计划解释为 FALSE。</li>
     * </ol>
     *
     * @param expression RuleProgram 和 PhysicalFilter 已校验的 canonical boolean expression
     * @param row 当前 cursor 位置的有效逻辑行
     * @return 保留 TRUE/FALSE/UNKNOWN 的 SQL boolean
     * @throws SqlExecutionException expression/row 缺失或物理形状未规范时抛出
     */
    public SqlBoolean evaluate(BoundExpression expression, SqlRowView row) {
        // 1、缺失输入是物理计划或 cursor 协议错误，不能降级为 WHERE 不匹配。
        if (expression == null || row == null) {
            throw new SqlExecutionException(
                    "expression evaluator requires expression and current row");
        }
        return switch (expression) {
            case BoundTruthLiteral truth -> truth.value();
            case BoundConjunction conjunction -> {
                // 2、FALSE 短路，UNKNOWN 仍需继续判断后续是否出现 FALSE。
                SqlBoolean result = SqlBoolean.TRUE;
                for (BoundExpression operand : conjunction.operands()) {
                    result = result.and(evaluate(operand, row));
                    if (result == SqlBoolean.FALSE) {
                        break;
                    }
                }
                yield result;
            }
            case BoundDisjunction disjunction -> {
                // 2、TRUE 短路，UNKNOWN 仍需继续判断后续是否出现 TRUE。
                SqlBoolean result = SqlBoolean.FALSE;
                for (BoundExpression operand : disjunction.operands()) {
                    result = result.or(evaluate(operand, row));
                    if (result == SqlBoolean.TRUE) {
                        break;
                    }
                }
                yield result;
            }
            case BoundNegation negation -> evaluate(negation.operand(), row).not();
            case BoundNullTest nullTest -> evaluateNullTest(nullTest, row);
            case BoundComparison comparison -> evaluateComparison(comparison, row);
            case BoundColumnReference ignored -> throw new SqlExecutionException(
                    "physical residual contains scalar column root");
            case BoundLiteral ignored -> throw new SqlExecutionException(
                    "physical residual contains scalar literal root");
        };
    }

    /**
     * 判断 WHERE 是否命中；FALSE 和 UNKNOWN 均被过滤。
     *
     * @param expression canonical boolean residual
     * @param row 当前有效逻辑行
     * @return 仅求值结果为 TRUE 时返回 {@code true}
     */
    public boolean matches(BoundExpression expression, SqlRowView row) {
        return evaluate(expression, row).matchesWhere();
    }

    /**
     * 求值 canonical column/literal NULL test，且不读取非 NULL 外置 payload。
     */
    private static SqlBoolean evaluateNullTest(
            BoundNullTest nullTest, SqlRowView row) {
        boolean isNull;
        if (nullTest.operand() instanceof BoundColumnReference column) {
            isNull = row.isNullAt(
                    column.relationOrdinal(),
                    column.columnOrdinal());
        } else if (nullTest.operand() instanceof BoundLiteral literal) {
            isNull = literal.value() instanceof SqlValue.NullValue;
        } else {
            throw new SqlExecutionException(
                    "physical null-test requires column or literal operand");
        }
        boolean matches = nullTest.operator() == BoundNullTestOperator.IS_NULL
                ? isNull : !isNull;
        return matches ? SqlBoolean.TRUE : SqlBoolean.FALSE;
    }

    /**
     * 使用 row view 的 exact type/collation comparator 求值 canonical comparison。
     */
    private static SqlBoolean evaluateComparison(
            BoundComparison comparison, SqlRowView row) {
        if (!(comparison.left()
                instanceof BoundColumnReference column)) {
            throw new SqlExecutionException(
                    "physical comparison requires a column left operand");
        }
        if (comparison.right()
                instanceof BoundColumnReference rightColumn) {
            if (row.isNullAt(
                    column.relationOrdinal(),
                    column.columnOrdinal())
                    || row.isNullAt(
                    rightColumn.relationOrdinal(),
                    rightColumn.columnOrdinal())) {
                return SqlBoolean.UNKNOWN;
            }
            SqlValue right = row.valueAt(
                    rightColumn.relationOrdinal(),
                    rightColumn.columnOrdinal());
            int order = row.compareLiteral(
                    column.relationOrdinal(),
                    column.columnOrdinal(), right);
            return matches(
                    comparison.operator(), order);
        }
        if (!(comparison.right()
                instanceof BoundLiteral literal)) {
            throw new SqlExecutionException(
                    "physical comparison right operand must be a column or literal");
        }
        if (literal.value() instanceof SqlValue.NullValue
                || row.isNullAt(
                column.relationOrdinal(),
                column.columnOrdinal())) {
            return SqlBoolean.UNKNOWN;
        }
        int order = row.compareLiteral(
                column.relationOrdinal(),
                column.columnOrdinal(), literal.value());
        return matches(
                comparison.operator(), order);
    }

    private static SqlBoolean matches(
            cn.zhangyis.db.sql.expression.BoundComparisonOperator operator,
            int order) {
        boolean matched = switch (operator) {
            case EQUAL -> order == 0;
            case LESS_THAN -> order < 0;
            case LESS_THAN_OR_EQUAL -> order <= 0;
            case GREATER_THAN -> order > 0;
            case GREATER_THAN_OR_EQUAL -> order >= 0;
        };
        return matched ? SqlBoolean.TRUE : SqlBoolean.FALSE;
    }
}
