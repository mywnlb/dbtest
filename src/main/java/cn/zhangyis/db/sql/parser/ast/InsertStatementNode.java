package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;
/**
 * 表示SQL 词法与语法解析中的 {@code InsertStatementNode} 不可变 AST 节点；保留源位置与语法值，binder 只读取该节点，不允许解析后就地改写。
 *
 * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param columns 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 * @param values 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 */
public record InsertStatementNode(QualifiedNameNode table, List<IdentifierNode> columns,
                                  List<LiteralNode> values) implements StatementNode {
    public InsertStatementNode {
        if (table == null || columns == null || columns.isEmpty() || values == null || columns.size() != values.size()) {
            throw new DatabaseValidationException("invalid INSERT AST shape");
        }
        columns = List.copyOf(columns); values = List.copyOf(values);
    }
}
