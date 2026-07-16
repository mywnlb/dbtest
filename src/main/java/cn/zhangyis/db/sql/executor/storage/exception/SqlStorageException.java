package cn.zhangyis.db.sql.executor.storage.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** SQL port 适配或内核执行失败；保留底层领域异常作为 cause，但不把物理对象放入 API。 */
public class SqlStorageException extends DatabaseRuntimeException {
    public SqlStorageException(String message) { super(message); }
    public SqlStorageException(String message, Throwable cause) { super(message, cause); }
}
