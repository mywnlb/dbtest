package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;

public record DeleteStatementNode(QualifiedNameNode table, List<EqualityPredicateNode> predicates) implements StatementNode {
    public DeleteStatementNode {
        if (table == null || predicates == null || predicates.isEmpty()) throw new DatabaseValidationException("invalid DELETE AST shape");
        predicates = List.copyOf(predicates);
    }
}
