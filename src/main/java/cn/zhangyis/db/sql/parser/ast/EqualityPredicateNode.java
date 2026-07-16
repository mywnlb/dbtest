package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
public record EqualityPredicateNode(IdentifierNode column, LiteralNode value) {
    public EqualityPredicateNode {
        if (column == null || value == null) throw new DatabaseValidationException("predicate fields must not be null");
    }
}
