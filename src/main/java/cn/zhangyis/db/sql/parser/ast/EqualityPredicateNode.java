package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
/**
 * 表示SQL 词法与语法解析中的 {@code EqualityPredicateNode} 不可变 AST 节点；保留源位置与语法值，binder 只读取该节点，不允许解析后就地改写。
 *
 * @param column SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
 * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
 */
public record EqualityPredicateNode(ColumnReferenceNode column, LiteralNode value) implements PredicateNode {
    public EqualityPredicateNode {
        if (column == null || value == null) throw new DatabaseValidationException("predicate fields must not be null");
    }

    public EqualityPredicateNode(IdentifierNode column, LiteralNode value) {
        this(new ColumnReferenceNode(column), value);
    }
}
