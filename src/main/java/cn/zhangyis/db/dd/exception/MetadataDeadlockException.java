package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 独立 metadata wait graph 成环；当前形成环的请求是 v1 确定性 victim。 */
public class MetadataDeadlockException extends DatabaseRuntimeException {
    public MetadataDeadlockException(String message) {
        super(message);
    }

    public MetadataDeadlockException(String message, Throwable cause) {
        super(message, cause);
    }
}
