package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 独立 CREATE INDEX 与 ALTER TABLE ADD INDEX 归一后的纯语法 AST。
 *
 * @param table 目标限定表名
 * @param indexName 索引标识符
 * @param unique 是否声明 UNIQUE
 * @param keyParts 一个或多个列 key part
 */
public record CreateIndexStatementNode(QualifiedNameNode table, IdentifierNode indexName,
                                       boolean unique, List<IndexKeyPartNode> keyParts)
        implements StatementNode {
    public CreateIndexStatementNode {
        if (table == null || indexName == null || keyParts == null || keyParts.isEmpty()) {
            throw new DatabaseValidationException("CREATE INDEX AST fields must not be null or empty");
        }
        keyParts = List.copyOf(keyParts);
    }
}
