package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;

public record UpdateStatementNode(QualifiedNameNode table, List<AssignmentNode> assignments,
                                  List<EqualityPredicateNode> predicates) implements StatementNode {
    public UpdateStatementNode {
        if (table == null || assignments == null || assignments.isEmpty() || predicates == null || predicates.isEmpty())
            throw new DatabaseValidationException("invalid UPDATE AST shape");
        assignments = List.copyOf(assignments); predicates = List.copyOf(predicates);
    }
}
