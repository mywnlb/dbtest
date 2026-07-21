package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/** recovery identity 缺失或损坏导致可信备份操作无法证明来源时抛出的致命领域异常。 */
public final class RecoveryIdentityException extends DatabaseFatalException {

    /** @param message 包含 identity 路径或格式上下文的诊断消息 */
    public RecoveryIdentityException(String message) {
        super(message);
    }

    /** @param message 诊断消息 @param cause 原始 IO/格式失败 */
    public RecoveryIdentityException(String message, Throwable cause) {
        super(message, cause);
    }
}
