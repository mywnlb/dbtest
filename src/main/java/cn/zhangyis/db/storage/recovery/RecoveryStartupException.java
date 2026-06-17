package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 启动恢复失败异常。Recovery 在 fail closed 后抛出该异常，保留原始 redo/doublewrite/page IO 根因。
 */
public class RecoveryStartupException extends DatabaseFatalException {

    /**
     * 创建恢复失败异常。
     *
     * @param message 恢复失败阶段和上下文。
     * @param cause 原始领域异常或底层异常。
     */
    public RecoveryStartupException(String message, Throwable cause) {
        super(message, cause);
    }
}
