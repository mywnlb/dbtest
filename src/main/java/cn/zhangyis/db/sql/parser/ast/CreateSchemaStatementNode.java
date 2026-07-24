package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * CREATE SCHEMA 与 CREATE DATABASE 的规范 AST。
 *
 * @param name schema 标识符
 * @param ifNotExists 已存在时是否转换为 warning
 */
public record CreateSchemaStatementNode(
        IdentifierNode name, boolean ifNotExists) implements StatementNode {

    public CreateSchemaStatementNode {
        if (name == null) {
            throw new DatabaseValidationException(
                    "CREATE SCHEMA name must not be null");
        }
    }
}
