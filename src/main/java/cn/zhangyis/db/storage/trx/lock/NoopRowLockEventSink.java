package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.domain.TransactionId;

import java.time.Duration;
import java.util.List;

/**
 * {@link RowLockEventSink} 的 no-op 实现。LockManager 未显式接线观测层时使用它，保证业务锁行为不变、
 * 不分配真实 thread/event id（返回 {@link ThreadEventId#NONE}）。诊断快照仍可由 LockManager 只读快照单独推导。
 */
enum NoopRowLockEventSink implements RowLockEventSink {
    INSTANCE;

    @Override
    public ThreadEventId openRowLockEvent(TransactionId owner, TransactionLockKey key,
                                          TransactionLockMode mode, long requestId) {
        return ThreadEventId.NONE;
    }

    @Override
    public void markRowLockWaiting(RowLockObservation observation, List<RowLockBlocker> blockers, Duration timeout) {
    }

    @Override
    public void markRowLockGranted(RowLockObservation observation) {
    }

    @Override
    public void markRowLockReleased(RowLockObservation observation, String releaseReason) {
    }

    @Override
    public void markRowLockTimeout(RowLockObservation observation) {
    }

    @Override
    public void markRowLockVictim(RowLockObservation observation, List<WaitForEdgeSnapshot> edges) {
    }
}
