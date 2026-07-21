package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 可信备份创建、签名、验证或导入证据不完整时抛出的可诊断领域异常。 */
public final class RecoveryBackupException extends DatabaseRuntimeException {

    /** @param message 含备份/table/space/path 上下文的诊断信息 */
    public RecoveryBackupException(String message) {
        super(message);
    }

    /**
     * @param message 含备份/table/space/path 上下文的诊断信息
     * @param cause 原始 IO、密码学或格式失败
     */
    public RecoveryBackupException(String message, Throwable cause) {
        super(message, cause);
    }
}
