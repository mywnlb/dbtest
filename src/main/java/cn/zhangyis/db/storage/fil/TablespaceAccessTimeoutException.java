package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 等待表空间共享/独占 operation lease 超时或被中断。调用方可回滚并重试。 */
public final class TablespaceAccessTimeoutException extends DatabaseRuntimeException {

    public TablespaceAccessTimeoutException(String message) {
        super(message);
    }

    public TablespaceAccessTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
