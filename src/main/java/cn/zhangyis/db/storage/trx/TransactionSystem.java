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

    /**
     * 为一致性读创建 {@link ReadView}（设计 §5.4/§8.1，T1.4）。**在同一临界区内原子完成**：
     * <ol>
     *   <li>若 {@code txn} 非只读且尚无写 id：分配并登记 creator 写 id（进活跃表）——使可写事务建 ReadView 后的自身写命中 creator 规则；</li>
     *   <li>原子捕获 {@code {activeIds, nextTransactionId}}：{@code lowLimitId = nextTransactionId}、
     *       {@code upLimitId = min(activeIds)}（空集合时 = low）、creator = {@code txn.transactionId()}（只读为 NONE）。</li>
     * </ol>
     * 必须原子：否则 active 集合与水位之间出现撕裂读，会破坏 {@link ReadView} 的 active⊆[up,low) 不变量。
     * 持锁期间只读写内存状态，不访问 Buffer Pool、不等待（§17）。
     *
     * @param txn 发起一致性读的事务（只读 creator 保持 NONE；可写则在此分配 creator）。
     * @return 该时刻的一致性读快照。
     */
    ReadView openReadViewSnapshot(Transaction txn) {
        if (txn == null) {
            throw new TransactionStateException("openReadViewSnapshot txn must not be null");
        }
        lock.lock();
        try {
            if (!txn.readOnly() && txn.transactionId().isNone()) {
                long id = nextTransactionId++;
                active.register(id);
                txn.setTransactionId(TransactionId.of(id));
            }
            Set<Long> ids = active.snapshot();
            long low = nextTransactionId;
            long up = low;
            for (long id : ids) {
                if (id < up) {
                    up = id;
                }
            }
            return new ReadView(txn.transactionId(), up, low, ids);
        } finally {
            lock.unlock();
        }
    }
}
