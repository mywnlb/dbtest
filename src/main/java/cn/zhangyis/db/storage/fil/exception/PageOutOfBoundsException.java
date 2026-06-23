package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 页号越界异常。请求的 pageNo 超过表空间当前物理大小。可恢复：调用方可先 extend 再重试。
 */
public class PageOutOfBoundsException extends DatabaseRuntimeException {

    public PageOutOfBoundsException(String message) {
        super(message);
    }

    public PageOutOfBoundsException(String message, Throwable cause) {
        super(message, cause);
    }
}
