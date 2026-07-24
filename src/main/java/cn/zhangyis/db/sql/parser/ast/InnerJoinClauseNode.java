package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * 二表等值 INNER JOIN 语法节点。Parser 只保证 ON 为两个列引用的等值，
 * 名称归属、类型兼容与索引能力由 Binder/Optimizer 决定。
 *
 * @param table SQL 右表限定名
 * @param alias 可选显式或隐式右表别名
 * @param leftColumn ON 等号左侧列引用
 * @param rightColumn ON 等号右侧列引用
 */
public record InnerJoinClauseNode(
        QualifiedNameNode table,
        Optional<IdentifierNode> alias,
        ColumnReferenceNode leftColumn,
        ColumnReferenceNode rightColumn) {

    public InnerJoinClauseNode {
        if (table == null || alias == null
                || leftColumn == null || rightColumn == null) {
            throw new DatabaseValidationException(
                    "INNER JOIN table/alias/columns must not be null");
        }
    }
}
