package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** history append/unlink 等待另一个跨 IO 转换结束超时；尚未修改任何新页面，可由上层重试。 */
public final class HistoryTransitionTimeoutException extends DatabaseRuntimeException {

    public HistoryTransitionTimeoutException(String message) {
        super(message);
    }
}
