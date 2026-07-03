package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * LockManager 的轻量只读快照。该对象用于测试与后续 lock observability 适配，列表均为防御性不可变副本；
 * 快照不承诺跨分片完全同一时刻，只保证每个条目来自某次受 mutex 保护的读取。
 *
 * @param grantedLocks 已授予事务锁。
 * @param waitingLocks 正在等待的事务锁请求。
 * @param waitEdges    row-lock wait-for graph 边。
 */
public record LockSnapshot(List<GrantedLockSnapshot> grantedLocks,
                           List<WaitingLockSnapshot> waitingLocks,
                           List<WaitForEdgeSnapshot> waitEdges) {

    public LockSnapshot {
        if (grantedLocks == null) {
            throw new DatabaseValidationException("grantedLocks must not be null");
        }
        if (waitingLocks == null) {
            throw new DatabaseValidationException("waitingLocks must not be null");
        }
        if (waitEdges == null) {
            throw new DatabaseValidationException("waitEdges must not be null");
        }
        grantedLocks = List.copyOf(grantedLocks);
        waitingLocks = List.copyOf(waitingLocks);
        waitEdges = List.copyOf(waitEdges);
    }
}
