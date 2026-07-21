package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * SQL {@code column BETWEEN lower AND upper} AST。两个端点在语法层均为闭边界；
 * Binder 随后按列类型转换并检查空区间，Parser 不比较不同词形的大小。
 *
 * @param column 未绑定列标识符；必须属于当前语句
 * @param lowerInclusive 闭合下界字面量
 * @param upperInclusive 闭合上界字面量
 */
public record BetweenPredicateNode(IdentifierNode column, LiteralNode lowerInclusive,
                                   LiteralNode upperInclusive) implements PredicateNode {
    public BetweenPredicateNode {
        if (column == null || lowerInclusive == null || upperInclusive == null) {
            throw new DatabaseValidationException("BETWEEN predicate fields must not be null");
        }
    }
}
