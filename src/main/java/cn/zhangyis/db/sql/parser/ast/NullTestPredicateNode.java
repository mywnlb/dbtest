package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 列 NULL 检查的原子 AST。
 *
 * @param column 尚未绑定 DD identity 的列名
 * @param operator IS NULL 或 IS NOT NULL；结果在 SQL 中始终为 TRUE/FALSE
 */
public record NullTestPredicateNode(
        ColumnReferenceNode column,
        NullTestOperator operator) implements PredicateNode {

    /**
     * 创建列 null-test；名称解析和 nullable 推导留给 Binder。
     *
     * @param column 原始列标识符
     * @param operator 显式 IS NULL/IS NOT NULL 操作符
     * @throws DatabaseValidationException 任一字段缺失时抛出
     */
    public NullTestPredicateNode {
        if (column == null || operator == null) {
            throw new DatabaseValidationException(
                    "null-test predicate fields must not be null");
        }
    }

    public NullTestPredicateNode(
            IdentifierNode column, NullTestOperator operator) {
        this(new ColumnReferenceNode(column), operator);
    }
}
