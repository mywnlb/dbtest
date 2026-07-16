package cn.zhangyis.db.sql.binder.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** SQL literal 不能无损转换为目标 DD 类型。 */
public final class SqlTypeCoercionException extends DatabaseRuntimeException {
    public SqlTypeCoercionException(String message) { super(message); }
    public SqlTypeCoercionException(String message, Throwable cause) { super(message, cause); }
}
