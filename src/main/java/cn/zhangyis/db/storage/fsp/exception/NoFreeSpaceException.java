package cn.zhangyis.db.storage.fsp.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间扩展一次后仍无可用空间（设计 §17）。可恢复：调用方可放弃/回滚当前操作。
 */
public class NoFreeSpaceException extends DatabaseRuntimeException {

    public NoFreeSpaceException(String message) {
        super(message);
    }

    public NoFreeSpaceException(String message, Throwable cause) {
        super(message, cause);
    }
}
