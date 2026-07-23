package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 同一目标页 Change Buffer buffer/merge/drain gate 在配置期限内无法取得时抛出的可重试异常。 */
public final class ChangeBufferPageGateTimeoutException extends DatabaseRuntimeException {

    /** @param message 包含目标页与等待上限的诊断信息 */
    public ChangeBufferPageGateTimeoutException(String message) {
        super(message);
    }

    /**
     * @param message 包含目标页与等待上限的诊断信息
     * @param cause 线程中断等原始失败
     */
    public ChangeBufferPageGateTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
