package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 单个 ORDER BY 项。
 *
 * @param column 当前单表语法中的列名；名称解析和类型判断留给 Binder
 * @param direction 显式或默认规范化后的 ASC/DESC
 */
public record OrderByItemNode(
        ColumnReferenceNode column, SortDirectionNode direction) {

    public OrderByItemNode {
        if (column == null || direction == null) {
            throw new DatabaseValidationException(
                    "ORDER BY column/direction must not be null");
        }
    }

    /**
     * 保留旧单表 AST 构造入口，并把裸标识符提升为未限定列引用。
     */
    public OrderByItemNode(
            IdentifierNode column, SortDirectionNode direction) {
        this(new ColumnReferenceNode(column), direction);
    }
}
