package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * row-lock wait-for graph 检测到环。当前切片采用“当前等待请求作为 victim”的简化策略：
 * LockManager 只移除该等待请求并抛异常，不自动驱动 TransactionManager rollback。
 */
public class DeadlockDetectedException extends DatabaseRuntimeException {

    public DeadlockDetectedException(String message) {
        super(message);
    }

    public DeadlockDetectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
