package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务全局协调器（innodb-transaction-mvcc-design §5.3、§17）。
 *
 * <p>并发：用一把 {@link ReentrantLock} 短锁保护 {@code nextTransactionId}/{@code nextTransactionNo} 与私有
 * {@link ActiveTransactionTable}。所有分配/登记/移除/快照均在锁内完成，{@link #snapshotActiveReadWriteIds()}
 * 拷贝后立即释放锁。**持锁期间不访问 Buffer Pool、不持 page latch、不等待**（§17 锁顺序约束），故该锁不会卷入
 * 任何可阻塞路径。禁止 {@code synchronized}。
 */
public final class TransactionSystem {

    /** 保护下列三项全局状态的短锁。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 下一个待分配事务 id（从 1 起；0 保留给 NONE）。 */
    private long nextTransactionId = 1;
    /** 下一个待分配提交序号（从 1 起；0 保留给 NONE）。 */
    private long nextTransactionNo = 1;
    /** 活跃读写事务表，仅在本类锁内驱动。 */
    private final ActiveTransactionTable active = new ActiveTransactionTable();

    /** 分配单调事务写 id 并登记为活跃读写事务（首次写入时调用）。 */
    TransactionId allocateWriteId() {
        lock.lock();
        try {
            long id = nextTransactionId++;
            active.register(id);
            return TransactionId.of(id);
        } finally {
            lock.unlock();
        }
    }

    /** 分配单调提交序号（commit 时给读写事务调用）。 */
    TransactionNo allocateTransactionNo() {
        lock.lock();
        try {
            return TransactionNo.of(nextTransactionNo++);
        } finally {
            lock.unlock();
        }
    }

    /** 从活跃表移出（commit/rollback 读写事务）。 */
    void removeActive(long txnId) {
        lock.lock();
        try {
            active.remove(txnId);
        } finally {
            lock.unlock();
        }
    }

    /** 活跃读写事务 id 的不可变快照（拷贝后立即释放锁）。 */
    public Set<Long> snapshotActiveReadWriteIds() {
        lock.lock();
        try {
            return active.snapshot();
        } finally {
            lock.unlock();
        }
    }
}
