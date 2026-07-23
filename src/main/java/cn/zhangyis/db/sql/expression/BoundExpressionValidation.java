package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;

/**
 * Bound expression 与 exact table version 的跨字段校验器。
 */
public final class BoundExpressionValidation {
    private BoundExpressionValidation() {
    }

    /**
     * 校验 boolean condition 中每个列引用的 id、ordinal 与 exact DD type。
     *
     * <ol>
     *     <li>拒绝缺失或非 boolean 根，避免 scalar 被误当作 WHERE。</li>
     *     <li>递归遍历 sealed expression，逐列核对 exact table version。</li>
     * </ol>
     *
     * @param condition Binder 或规则产生的 WHERE condition
     * @param table statement metadata lease 固定的 exact table version
     * @throws DatabaseValidationException 根类型非法，或列引用与 table version 不一致时抛出
     */
    public static void validateCondition(
            BoundExpression condition, TableDefinition table) {
        // 1、WHERE 根必须显式拥有 boolean 三值语义。
        if (condition == null || table == null
                || !(condition.type()
                instanceof BoundExpressionType.BooleanResult)) {
            throw new DatabaseValidationException(
                    "bound condition/table must be a non-null boolean binding");
        }
        // 2、规则重建节点后仍必须保持 column id/ordinal/type 三者一致。
        validateExpression(condition, table);
    }

    private static void validateExpression(
            BoundExpression expression, TableDefinition table) {
        switch (expression) {
            case BoundColumnReference column -> {
                if (column.columnOrdinal() >= table.columns().size()) {
                    throw new DatabaseValidationException(
                            "bound column ordinal exceeds exact table width");
                }
                var exact = table.columns().get(column.columnOrdinal());
                if (exact.columnId() != column.columnId()
                        || !exact.type().equals(column.columnType())) {
                    throw new DatabaseValidationException(
                            "bound column identity/type differs from exact table version");
                }
            }
            case BoundLiteral ignored -> {
                // literal 的 exact scalar type 已由 comparison 构造器与另一侧交叉校验。
            }
            case BoundComparison comparison -> {
                validateExpression(comparison.left(), table);
                validateExpression(comparison.right(), table);
            }
            case BoundConjunction conjunction ->
                    conjunction.operands().forEach(
                            operand -> validateExpression(operand, table));
            case BoundDisjunction disjunction ->
                    disjunction.operands().forEach(
                            operand -> validateExpression(operand, table));
            case BoundNegation negation ->
                    validateExpression(negation.operand(), table);
            case BoundNullTest nullTest ->
                    validateExpression(nullTest.operand(), table);
            case BoundTruthLiteral ignored -> {
                // truth literal 不引用 table column。
            }
        }
    }
}
