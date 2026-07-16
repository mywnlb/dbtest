package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;
public record SelectStatementNode(boolean star, List<IdentifierNode> projections, QualifiedNameNode table,
                                  List<EqualityPredicateNode> predicates) implements StatementNode {
    public SelectStatementNode {
        if (projections == null || table == null || predicates == null || predicates.isEmpty()
                || star != projections.isEmpty()) throw new DatabaseValidationException("invalid SELECT AST shape");
        projections = List.copyOf(projections); predicates = List.copyOf(predicates);
    }
}
