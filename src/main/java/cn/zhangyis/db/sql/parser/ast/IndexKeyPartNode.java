package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * CREATE INDEX 的单个列 key part。
 *
 * @param column 纯语法列名，具体 columnId 由持 MDL X 的 DD coordinator 解析
 * @param order 显式或默认后的 ASC/DESC
 */
public record IndexKeyPartNode(IdentifierNode column, IndexKeyOrderNode order) {
    public IndexKeyPartNode {
        if (column == null || order == null) {
            throw new DatabaseValidationException("index key part column/order must not be null");
        }
    }
}
