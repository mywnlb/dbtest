package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** INSERT 需要 external LOB，但精确 DD table binding 没有可用 LOB segment。 */
public final class DmlLobBindingException extends DatabaseRuntimeException {
    public DmlLobBindingException(String message) {
        super(message);
    }

    public DmlLobBindingException(String message, Throwable cause) {
        super(message, cause);
    }
}
