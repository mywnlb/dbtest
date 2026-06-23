package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 等待目标表空间 fixed frame 释放超时/中断；调用方可保持 TRUNCATING 并在恢复或重试中续作。 */
public final class BufferPoolInvalidationTimeoutException extends DatabaseRuntimeException {

    public BufferPoolInvalidationTimeoutException(String message) {
        super(message);
    }

    public BufferPoolInvalidationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
