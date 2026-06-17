package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Flush/doublewrite/data-file 写盘路径的可恢复运行时异常。调用方不得把失败页标 clean，
 * 可以保留 dirty 并在后续 flush 轮次重试，或把错误上报给恢复/只读降级流程。
 */
public class FlushWriteException extends DatabaseRuntimeException {

    public FlushWriteException(String message) {
        super(message);
    }

    public FlushWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
