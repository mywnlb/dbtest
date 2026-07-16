package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** MDL 等待超时或线程被中断；抛出前 request 已从 queue/graph 清理。 */
public class MetadataLockTimeoutException extends DatabaseRuntimeException {
    public MetadataLockTimeoutException(String message) {
        super(message);
    }

    public MetadataLockTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
