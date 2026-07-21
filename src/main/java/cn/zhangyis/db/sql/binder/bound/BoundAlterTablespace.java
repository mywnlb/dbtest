package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.sql.parser.ast.AlterTablespaceStatementNode;

/**
 * 已补全 schema 的 DISCARD/IMPORT TABLESPACE 意图；对象不持有 DD lease 或物理文件句柄。
 *
 * @param table 由 Binder 规范化的稳定逻辑表名
 * @param action 请求的表空间生命周期动作
 */
public record BoundAlterTablespace(QualifiedTableName table,
                                   AlterTablespaceStatementNode.Action action)
        implements BoundStatement {
    public BoundAlterTablespace {
        if (table == null || action == null) {
            throw new DatabaseValidationException("bound ALTER TABLESPACE fields must not be null");
        }
    }
}
