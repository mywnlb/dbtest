package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;
public record InsertStatementNode(QualifiedNameNode table, List<IdentifierNode> columns,
                                  List<LiteralNode> values) implements StatementNode {
    public InsertStatementNode {
        if (table == null || columns == null || columns.isEmpty() || values == null || columns.size() != values.size()) {
            throw new DatabaseValidationException("invalid INSERT AST shape");
        }
        columns = List.copyOf(columns); values = List.copyOf(values);
    }
}
