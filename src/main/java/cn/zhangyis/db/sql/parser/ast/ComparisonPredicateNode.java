package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 非等值二元 comparison AST；列始终位于左侧，右侧只接受 literal。
 *
 * @param column 未绑定列标识符；必须属于当前语句
 * @param operator 开放或闭合、上界或下界的比较方向
 * @param value 尚未按 DD 列类型转换的 SQL 字面量
 */
public record ComparisonPredicateNode(ColumnReferenceNode column, ComparisonOperator operator,
                                      LiteralNode value) implements PredicateNode {
    public ComparisonPredicateNode {
        if (column == null || operator == null || value == null) {
            throw new DatabaseValidationException("comparison predicate fields must not be null");
        }
    }

    public ComparisonPredicateNode(
            IdentifierNode column, ComparisonOperator operator,
            LiteralNode value) {
        this(new ColumnReferenceNode(column), operator, value);
    }
}
