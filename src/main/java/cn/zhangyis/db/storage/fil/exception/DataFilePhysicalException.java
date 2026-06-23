package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 数据文件物理 IO 异常。positional 读写、文件创建或扩展失败时抛出，必须把底层 IOException 作为 cause 保留。
 * 可恢复：调用方可重试、上报或关闭资源。
 */
public class DataFilePhysicalException extends DatabaseRuntimeException {

    public DataFilePhysicalException(String message) {
        super(message);
    }

    public DataFilePhysicalException(String message, Throwable cause) {
        super(message, cause);
    }
}
