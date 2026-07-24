package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;

import java.util.List;

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
        if (table == null) {
            throw new DatabaseValidationException(
                    "bound condition/table must be a non-null boolean binding");
        }
        validateCondition(condition, List.of(table));
    }

    /**
     * 校验多关系 condition 中每个列引用的 relation/local ordinal 与 exact DD 身份。
     *
     * @param condition Binder 或规则产生的 boolean condition
     * @param tables 按 statement relation ordinal 排列的 exact table versions
     * @throws DatabaseValidationException 根类型或任一列身份不属于当前 statement schema 时抛出
     */
    public static void validateCondition(
            BoundExpression condition, List<TableDefinition> tables) {
        // 1、WHERE 根必须显式拥有 boolean 三值语义。
        if (condition == null || tables == null || tables.isEmpty()
                || tables.stream().anyMatch(java.util.Objects::isNull)
                || !(condition.type()
                instanceof BoundExpressionType.BooleanResult)) {
            throw new DatabaseValidationException(
                    "bound condition/table must be a non-null boolean binding");
        }
        // 2、规则重建节点后仍必须保持 column id/ordinal/type 三者一致。
        validateExpression(condition, List.copyOf(tables));
    }

    private static void validateExpression(
            BoundExpression expression, List<TableDefinition> tables) {
        switch (expression) {
            case BoundColumnReference column -> {
                if (column.relationOrdinal() >= tables.size()) {
                    throw new DatabaseValidationException(
                            "bound relation ordinal exceeds statement input count");
                }
                TableDefinition table =
                        tables.get(column.relationOrdinal());
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
                validateExpression(comparison.left(), tables);
                validateExpression(comparison.right(), tables);
            }
            case BoundConjunction conjunction ->
                    conjunction.operands().forEach(
                            operand -> validateExpression(operand, tables));
            case BoundDisjunction disjunction ->
                    disjunction.operands().forEach(
                            operand -> validateExpression(operand, tables));
            case BoundNegation negation ->
                    validateExpression(negation.operand(), tables);
            case BoundNullTest nullTest ->
                    validateExpression(nullTest.operand(), tables);
            case BoundTruthLiteral ignored -> {
                // truth literal 不引用 table column。
            }
        }
    }
}
