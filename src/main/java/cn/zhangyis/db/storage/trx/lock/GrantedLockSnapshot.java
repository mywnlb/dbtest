package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 已授予事务锁的只读快照项。用于后续 server.lockobs 适配；它复制 LockManager 状态，不暴露内部请求对象。
 *
 * @param requestId     LockManager 请求 id，供观测层生成稳定 engine lock id。
 * @param threadEventId 请求创建时分配的 thread/event id；未接观测时为 NONE。
 * @param owner 持锁事务。
 * @param key   锁资源。
 * @param mode  锁模式。
 * @param state 快照时状态，当前应为 {@link TransactionLockState#GRANTED}。
 */
public record GrantedLockSnapshot(long requestId, ThreadEventId threadEventId,
                                  TransactionId owner, TransactionLockKey key,
                                  TransactionLockMode mode, TransactionLockState state) {

    public GrantedLockSnapshot {
        if (requestId < 0) {
            throw new DatabaseValidationException("granted lock request id must be non-negative");
        }
        if (threadEventId == null) {
            throw new DatabaseValidationException("granted lock thread event id must not be null");
        }
        if (owner == null || owner.isNone()) {
            throw new DatabaseValidationException("granted lock owner must be a real transaction id");
        }
        if (key == null) {
            throw new DatabaseValidationException("granted lock key must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("granted lock mode must not be null");
        }
        if (state == null) {
            throw new DatabaseValidationException("granted lock state must not be null");
        }
    }

    public GrantedLockSnapshot(TransactionId owner, TransactionLockKey key,
                               TransactionLockMode mode, TransactionLockState state) {
        this(0, ThreadEventId.NONE, owner, key, mode, state);
    }
}
