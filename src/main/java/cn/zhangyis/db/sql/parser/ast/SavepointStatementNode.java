package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 命名事务保存点 AST。Parser 只保留命令种类、用户名称及源位置；名称规范化、事务归属、
 * undo 边界与锁释放策略均由 Session/storage 在各自层内处理。
 *
 * @param kind 保存点命令种类；不得为 {@code null}
 * @param name 用户提供的保存点标识符；不得为 {@code null} 或空白
 */
public record SavepointStatementNode(Kind kind, IdentifierNode name) implements StatementNode {

    public SavepointStatementNode {
        if (kind == null || name == null) {
            throw new DatabaseValidationException("savepoint statement kind/name must not be null");
        }
    }

    /** 命名保存点的三种 SQL 生命周期动作。 */
    public enum Kind {
        /** 创建新边界；同名边界由 Session 按 SQL 语义替换。 */
        CREATE,
        /** 撤销目标边界之后的修改，同时保留目标名称供后续再次使用。 */
        ROLLBACK_TO,
        /** 仅释放目标名称与运行期边界，不修改 undo 链。 */
        RELEASE
    }
}
