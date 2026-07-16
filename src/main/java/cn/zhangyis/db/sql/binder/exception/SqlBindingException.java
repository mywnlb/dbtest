package cn.zhangyis.db.sql.binder.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** SQL 名称、shape 或资源生命周期无法形成可执行 bound statement。 */
public class SqlBindingException extends DatabaseRuntimeException {
    public SqlBindingException(String message) { super(message); }
    public SqlBindingException(String message, Throwable cause) { super(message, cause); }
}
