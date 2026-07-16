package cn.zhangyis.db.session.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** Session 当前生命周期或 transaction mode 不允许请求。 */
public class SessionStateException extends DatabaseRuntimeException {
    public SessionStateException(String message) { super(message); }
    public SessionStateException(String message, Throwable cause) { super(message, cause); }
}
