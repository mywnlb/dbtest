package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** DD sidecar/catalog IO 失败；保留原始 cause，由 DDL coordinator 决定回滚或 fail-closed。 */
public class DictionaryPersistenceException extends DatabaseRuntimeException {
    public DictionaryPersistenceException(String message) {
        super(message);
    }

    public DictionaryPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
