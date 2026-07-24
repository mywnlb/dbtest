package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * DROP SCHEMA 与 DROP DATABASE 的规范 AST。
 *
 * @param name schema 标识符
 * @param ifExists 缺失时是否转换为 warning
 */
public record DropSchemaStatementNode(
        IdentifierNode name, boolean ifExists) implements StatementNode {

    public DropSchemaStatementNode {
        if (name == null) {
            throw new DatabaseValidationException(
                    "DROP SCHEMA name must not be null");
        }
    }
}
