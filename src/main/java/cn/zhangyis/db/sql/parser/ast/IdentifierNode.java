package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

/** 保留用户拼写的标识符与起始位置；canonicalization 属于 Binder/DD。 */
public record IdentifierNode(String value, SourcePosition position) {
    public IdentifierNode {
        if (value == null || value.isBlank() || position == null) {
            throw new DatabaseValidationException("identifier value/position must not be blank/null");
        }
    }
}
