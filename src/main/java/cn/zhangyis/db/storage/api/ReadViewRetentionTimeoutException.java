package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** SHADOW finalization在调用方预算内仍存在cutover前ReadView时的可重试异常。 */
public final class ReadViewRetentionTimeoutException extends DatabaseRuntimeException {

    /** @param message 包含generation fence的诊断消息 */
    public ReadViewRetentionTimeoutException(String message) {
        super(message);
    }
}
