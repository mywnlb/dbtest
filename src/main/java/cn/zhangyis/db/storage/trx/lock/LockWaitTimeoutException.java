package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 事务锁等待超时。LockManager 已在抛出前清理等待队列和 wait-for graph，调用方可选择回滚事务、
 * 释放已持有锁或把错误返回给 SQL/session 层。
 */
public class LockWaitTimeoutException extends DatabaseRuntimeException {

    public LockWaitTimeoutException(String message) {
        super(message);
    }

    public LockWaitTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
