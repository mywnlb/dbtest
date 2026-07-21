package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * {@code ALTER TABLE ... DISCARD/IMPORT TABLESPACE} 的纯语法节点。
 *
 * @param table 目标表的语法限定名；单段名称由 Binder 使用 Session 当前 schema 补全
 * @param action 表空间脱离或重新接入动作；不得为 {@code null}
 */
public record AlterTablespaceStatementNode(QualifiedNameNode table, Action action)
        implements StatementNode {

    /** 表空间生命周期动作；两种动作均由 DD 在 table MDL X 下重验当前状态。 */
    public enum Action {
        /** 将 ACTIVE 物理文件移入实例受控的 discarded 目录。 */
        DISCARD,
        /** 从实例受控的 incoming 目录校验并重新挂载文件。 */
        IMPORT
    }

    public AlterTablespaceStatementNode {
        if (table == null || action == null) {
            throw new DatabaseValidationException("ALTER TABLESPACE AST fields must not be null");
        }
    }
}
