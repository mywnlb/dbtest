package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

public record AssignmentNode(IdentifierNode column, LiteralNode value) {
    public AssignmentNode {
        if (column == null || value == null) throw new DatabaseValidationException("assignment fields must not be null");
    }
}
