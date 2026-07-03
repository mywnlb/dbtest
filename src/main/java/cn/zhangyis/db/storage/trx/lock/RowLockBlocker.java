package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 一个 row-lock 等待请求的阻塞者摘要。LockManager 在持有自身锁表时复制出不可变 blocker 信息；
 * 观测层后续只能用这些值生成诊断行，不能反向访问 LockManager 内部队列。
 *
 * <p>作为 {@link RowLockEventSink} 端口的事件载荷定义在 storage 锁层，保证依赖只从 server.lockobs 向下指向 storage。
 *
 * @param requestId     阻塞锁请求 id。
 * @param owner         阻塞事务 id。
 * @param key           阻塞锁资源。
 * @param mode          阻塞锁模式。
 * @param threadEventId 阻塞锁的诊断事件 id；旧 no-op 路径可为 {@link ThreadEventId#NONE}。
 */
public record RowLockBlocker(long requestId, TransactionId owner, TransactionLockKey key,
                             TransactionLockMode mode, ThreadEventId threadEventId) {

    public RowLockBlocker {
        if (requestId <= 0) {
            throw new DatabaseValidationException("blocker request id must be positive: " + requestId);
        }
        if (owner == null || owner.isNone()) {
            throw new DatabaseValidationException("blocker owner must be a real transaction id");
        }
        if (key == null) {
            throw new DatabaseValidationException("blocker key must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("blocker mode must not be null");
        }
        if (threadEventId == null) {
            throw new DatabaseValidationException("blocker thread event id must not be null");
        }
    }
}
