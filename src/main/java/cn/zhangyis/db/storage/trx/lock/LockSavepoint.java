package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * LockManager 的轻量获取边界。序号来自全局单调 request id，回滚时仍按事务 owner 过滤，
 * 因而并发事务的请求不会被错误释放；对象不持有锁表分片、Condition 或等待边。
 *
 * @param acquisitionSequence 创建边界时已分配的最后一个锁请求序号；允许 0 表示尚无请求
 */
public record LockSavepoint(long acquisitionSequence) {
    public LockSavepoint {
        if (acquisitionSequence < 0) {
            throw new DatabaseValidationException("lock savepoint sequence must be non-negative");
        }
    }
}
