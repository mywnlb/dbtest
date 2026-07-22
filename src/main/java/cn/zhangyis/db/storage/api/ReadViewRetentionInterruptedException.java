package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** SHADOW finalization等待ReadView退出时线程被中断；调用方必须恢复capture或保留前滚证据。 */
public final class ReadViewRetentionInterruptedException extends DatabaseRuntimeException {

    /**
     * @param message 包含generation fence的诊断消息
     * @param cause 原始InterruptedException
     */
    public ReadViewRetentionInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
