package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 正在等待的事务锁请求快照项。调用方只能观察 owner/key/mode/state，不能通过快照唤醒或修改 wait queue。
 *
 * @param requestId     LockManager 请求 id，供观测层生成稳定 engine lock id。
 * @param threadEventId 请求创建时分配的 thread/event id；未接观测时为 NONE。
 * @param owner 等待事务。
 * @param key   等待资源。
 * @param mode  等待模式。
 * @param state 快照时状态，通常为 {@link TransactionLockState#WAITING}。
 */
public record WaitingLockSnapshot(long requestId, ThreadEventId threadEventId,
                                  TransactionId owner, TransactionLockKey key,
                                  TransactionLockMode mode, TransactionLockState state) {

    public WaitingLockSnapshot {
        if (requestId < 0) {
            throw new DatabaseValidationException("waiting lock request id must be non-negative");
        }
        if (threadEventId == null) {
            throw new DatabaseValidationException("waiting lock thread event id must not be null");
        }
        if (owner == null || owner.isNone()) {
            throw new DatabaseValidationException("waiting lock owner must be a real transaction id");
        }
        if (key == null) {
            throw new DatabaseValidationException("waiting lock key must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("waiting lock mode must not be null");
        }
        if (state == null) {
            throw new DatabaseValidationException("waiting lock state must not be null");
        }
    }

    public WaitingLockSnapshot(TransactionId owner, TransactionLockKey key,
                               TransactionLockMode mode, TransactionLockState state) {
        this(0, ThreadEventId.NONE, owner, key, mode, state);
    }
}
