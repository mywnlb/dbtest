package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * row-lock wait-for graph 的只读边。边方向为 waiting -> blocking；物理 latch、redo wait、file lock 不进入该图。
 *
 * @param waitingRequestId        正在等待的 LockManager 请求 id。
 * @param blockingRequestId       阻塞该请求的已授予请求 id。
 * @param waitingThreadEventId    等待请求的 thread/event id。
 * @param blockingThreadEventId   阻塞请求的 thread/event id。
 * @param waitingTransactionId  正在等待的事务。
 * @param blockingTransactionId 阻塞它的已持锁事务。
 * @param waitingKey            等待的锁资源。
 * @param waitingMode           等待的锁模式。
 * @param blockingKey           阻塞锁资源。
 * @param blockingMode          阻塞锁模式。
 */
public record WaitForEdgeSnapshot(long waitingRequestId, long blockingRequestId,
                                  ThreadEventId waitingThreadEventId, ThreadEventId blockingThreadEventId,
                                  TransactionId waitingTransactionId, TransactionId blockingTransactionId,
                                  TransactionLockKey waitingKey, TransactionLockMode waitingMode,
                                  TransactionLockKey blockingKey, TransactionLockMode blockingMode) {

    public WaitForEdgeSnapshot {
        if (waitingRequestId < 0 || blockingRequestId < 0) {
            throw new DatabaseValidationException("wait edge request ids must be non-negative");
        }
        if (waitingThreadEventId == null || blockingThreadEventId == null) {
            throw new DatabaseValidationException("wait edge thread event ids must not be null");
        }
        if (waitingTransactionId == null || waitingTransactionId.isNone()) {
            throw new DatabaseValidationException("wait edge waiting transaction must be real");
        }
        if (blockingTransactionId == null || blockingTransactionId.isNone()) {
            throw new DatabaseValidationException("wait edge blocking transaction must be real");
        }
        if (waitingKey == null) {
            throw new DatabaseValidationException("wait edge key must not be null");
        }
        if (waitingMode == null) {
            throw new DatabaseValidationException("wait edge mode must not be null");
        }
        if (blockingKey == null) {
            throw new DatabaseValidationException("wait edge blocking key must not be null");
        }
        if (blockingMode == null) {
            throw new DatabaseValidationException("wait edge blocking mode must not be null");
        }
    }

    public WaitForEdgeSnapshot(TransactionId waitingTransactionId, TransactionId blockingTransactionId,
                               TransactionLockKey waitingKey, TransactionLockMode waitingMode) {
        this(0, 0, ThreadEventId.NONE, ThreadEventId.NONE, waitingTransactionId, blockingTransactionId,
                waitingKey, waitingMode, waitingKey, waitingMode);
    }
}
