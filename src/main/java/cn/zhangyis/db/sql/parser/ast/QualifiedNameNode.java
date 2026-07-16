package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;

/** 一至三段 table name。三段名的 def catalog 限制由 parser 立即验证。 */
public record QualifiedNameNode(List<IdentifierNode> parts) {
    public QualifiedNameNode {
        if (parts == null || parts.isEmpty() || parts.size() > 3 || parts.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("qualified name must contain 1..3 parts");
        }
        parts = List.copyOf(parts);
    }
}
