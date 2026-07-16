package cn.zhangyis.db.sql.binder.exception;

/** SQL 标识符在 exact TableDefinition 中不存在。 */
public final class UnknownColumnException extends SqlBindingException {
    public UnknownColumnException(String message) { super(message); }
}
