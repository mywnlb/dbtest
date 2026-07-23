package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
/**
 * 表示SQL 词法与语法解析中的 {@code DeleteStatementNode} 不可变 AST 节点；保留源位置与语法值，binder 只读取该节点，不允许解析后就地改写。
 *
 * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param condition WHERE 的唯一权威 boolean 语法树；不得为 {@code null}
 */
public record DeleteStatementNode(
        QualifiedNameNode table,
        BooleanExpressionNode condition) implements StatementNode {
    public DeleteStatementNode {
        if (table == null || condition == null) {
            throw new DatabaseValidationException(
                    "invalid DELETE AST shape");
        }
    }
}
