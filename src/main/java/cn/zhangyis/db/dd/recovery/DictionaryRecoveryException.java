package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/** DD/物理绑定不一致或 DDL 恢复无法安全续作时的 fail-closed 异常。 */
public final class DictionaryRecoveryException extends DatabaseFatalException {
    public DictionaryRecoveryException(String message) {
        super(message);
    }

    public DictionaryRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
