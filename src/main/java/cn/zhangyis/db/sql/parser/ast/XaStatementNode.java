package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * Server XA 语句 AST。不同 kind 的合法选项在构造时交叉校验，Session 不需要从布尔组合猜测语法。
 *
 * @param kind XA 命令种类
 * @param xid START/END/PREPARE/COMMIT/ROLLBACK 必有；RECOVER 为空
 * @param startMode 仅 START/BEGIN 使用
 * @param endMode 仅 END 使用
 * @param onePhase 仅 COMMIT 可为 true
 * @param convertXid 仅 RECOVER 可为 true
 */
public record XaStatementNode(Kind kind, Optional<XaIdentifierNode> xid,
                              StartMode startMode, EndMode endMode,
                              boolean onePhase, boolean convertXid) implements StatementNode {

    public XaStatementNode {
        if (kind == null || xid == null || startMode == null || endMode == null) {
            throw new DatabaseValidationException("XA statement fields must not be null");
        }
        boolean recover = kind == Kind.RECOVER;
        if (recover == xid.isPresent()) {
            throw new DatabaseValidationException("XA RECOVER is the only command without XID");
        }
        if (kind != Kind.START && startMode != StartMode.NONE
                || kind != Kind.END && endMode != EndMode.NONE
                || kind != Kind.COMMIT && onePhase
                || kind != Kind.RECOVER && convertXid) {
            throw new DatabaseValidationException("XA statement options do not match command kind");
        }
    }

    /** XA 命令类别。 */
    public enum Kind {
        START,
        END,
        PREPARE,
        COMMIT,
        ROLLBACK,
        RECOVER
    }

    /** START/BEGIN 的活动 branch 恢复选项。 */
    public enum StartMode {
        NONE,
        JOIN,
        RESUME
    }

    /** END 的 suspend 语义；FOR_MIGRATE 语法可识别但当前 Session 明确拒绝跨会话迁移。 */
    public enum EndMode {
        NONE,
        SUSPEND,
        FOR_MIGRATE
    }
}
