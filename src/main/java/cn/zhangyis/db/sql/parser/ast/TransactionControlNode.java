package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
public record TransactionControlNode(Kind kind) implements StatementNode {
    public TransactionControlNode {
        if (kind == null) throw new DatabaseValidationException("transaction command kind must not be null");
    }
    public enum Kind { BEGIN, COMMIT, ROLLBACK }
}
