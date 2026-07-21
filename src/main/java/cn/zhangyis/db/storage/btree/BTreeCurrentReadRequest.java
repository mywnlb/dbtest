package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.trx.IsolationLevel;

import java.time.Duration;

/**
 * B+Tree current-read 请求上下文。它把逻辑事务 owner、隔离级别、行锁等待上限和重定位重试次数集中传入，
 * 避免 B+Tree 直接依赖 Transaction 聚合对象。
 *
 * @param owner                申请事务锁的真实事务 id，不能为 NONE。
 * @param isolationLevel       当前读锁策略使用的隔离级别；RU 沿用 RC、SERIALIZABLE 沿用 RR 锁范围。
 * @param lockWaitTimeout      单次 LockManager 等待上限。
 * @param maxRelocationRetries 授锁后发现记录/gap 已变化时最多重新定位次数。
 */
public record BTreeCurrentReadRequest(TransactionId owner, IsolationLevel isolationLevel,
                                      Duration lockWaitTimeout, int maxRelocationRetries) {

    public BTreeCurrentReadRequest {
        if (owner == null || owner.isNone()) {
            throw new DatabaseValidationException("current-read owner must be a real transaction id");
        }
        if (isolationLevel == null) {
            throw new DatabaseValidationException("current-read isolation level must not be null");
        }
        if (lockWaitTimeout == null || lockWaitTimeout.isZero() || lockWaitTimeout.isNegative()) {
            throw new DatabaseValidationException("current-read lock wait timeout must be positive");
        }
        if (maxRelocationRetries <= 0) {
            throw new DatabaseValidationException(
                    "current-read maxRelocationRetries must be positive: " + maxRelocationRetries);
        }
    }
}
