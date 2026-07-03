package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 单个事务锁请求的观测元数据。它不是锁真相来源，只是把 LockManager 的 requestId、事务、资源和线程事件
 * 固定成不可变值，供 wait slot、snapshot row 和 deadlock report 使用。
 *
 * <p>作为 {@link RowLockEventSink} 端口的事件载荷定义在 storage 锁层：LockManager 构造它并向观测端口发布，
 * server.lockobs 只消费，不反向被底层依赖。
 *
 * @param requestId     LockManager 内部请求 id，单进程生命周期内唯一。
 * @param owner         请求所属事务。
 * @param key           请求锁资源。
 * @param mode          请求锁模式。
 * @param threadEventId 创建请求的线程事件 id。
 */
public record RowLockObservation(long requestId, TransactionId owner, TransactionLockKey key,
                                 TransactionLockMode mode, ThreadEventId threadEventId) {

    public RowLockObservation {
        if (requestId <= 0) {
            throw new DatabaseValidationException("row lock request id must be positive: " + requestId);
        }
        if (owner == null || owner.isNone()) {
            throw new DatabaseValidationException("row lock owner must be a real transaction id");
        }
        if (key == null) {
            throw new DatabaseValidationException("row lock key must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("row lock mode must not be null");
        }
        if (threadEventId == null) {
            throw new DatabaseValidationException("row lock thread event id must not be null");
        }
    }

    /** 生成当前教学实现的稳定诊断锁 id；外部只能当作不透明字符串使用。 */
    public String engineLockId() {
        return "INNODB:" + requestId;
    }
}
