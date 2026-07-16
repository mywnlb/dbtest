package cn.zhangyis.db.sql.binder.exception;

/** 语法合法但超出 primary-point v1 确定性执行范围。 */
public final class UnsupportedSqlShapeException extends SqlBindingException {
    public UnsupportedSqlShapeException(String message) { super(message); }
}
