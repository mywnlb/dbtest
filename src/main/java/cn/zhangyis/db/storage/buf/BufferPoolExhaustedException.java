package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Buffer Pool 帧耗尽异常。需要新帧但所有帧都被 fix（无可淘汰受害者）时抛出。
 * 可恢复：调用方释放 PageGuard 后可重试。
 */
public class BufferPoolExhaustedException extends DatabaseRuntimeException {

    public BufferPoolExhaustedException(String message) {
        super(message);
    }

    public BufferPoolExhaustedException(String message, Throwable cause) {
        super(message, cause);
    }
}
