package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

import java.util.HashSet;
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
    /**
     * 存活一致性读 ReadView 集合（purge 低水位用，T-purge）。仅在本类锁内增删。ReadView 未重写 equals，
     * 故按对象身份去重——同一快照对象登记一次、注销一次。RR 由 {@link ReadViewManager#release} 注销，
     * RC 由调用方语句末经 {@link ReadViewManager#closeReadView} 注销；未注销表示该快照仍可能需要旧版本。
     */
    private final Set<ReadView> liveReadViews = new HashSet<>();

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

    /**
     * 恢复期登记一个已经持久化的 PREPARED creator。调用方必须先恢复 next-counter，且 recovery gate 仍关闭；
     * 本方法只在短锁内更新 active table，不访问 undo/page/redo。重复 creator 由 active table 以领域异常拒绝，
     * 避免两个运行时聚合共同拥有同一持久事务。
     *
     * @param transactionId page3/undo/redo 已交叉校验的正事务 id
     * @throws TransactionStateException id 缺失、NONE 或重复登记时抛出，恢复必须 fail-closed
     */
    void restorePreparedActive(TransactionId transactionId) {
        if (transactionId == null || transactionId.isNone()) {
            throw new TransactionStateException("recovered prepared transaction id must be assigned");
        }
        lock.lock();
        try {
            if (active.contains(transactionId.value())) {
                throw new TransactionStateException(
                        "recovered prepared transaction is already active: "
                                + transactionId.value());
            }
            active.register(transactionId.value());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 恢复期复位 id/no 计数器（R 1.3，单调只前进）。崩溃后由 restored undo 段算出的高水位驱动：
     * {@code nextTransactionId = max(creator TRANSACTION_ID)+1}、{@code nextTransactionNo = max(COMMIT_NO)+1}。
     * 使恢复后新事务 id/no 不与 pre-crash 已提交事务重复，且 {@link #purgeLowWaterNo}（= nextTransactionNo）覆盖
     * 重建的 committed history（其 TransactionNo 必 &lt; 复位值）→ 后台 purge 可续作。锁内只写内存，不访问 IO。
     *
     * @param nextTransactionId 复位后下一个待分配写 id（&gt;=1）。
     * @param nextTransactionNo 复位后下一个待分配提交序号（&gt;=1）。
     */
    public void restoreCounters(long nextTransactionId, long nextTransactionNo) {
        if (nextTransactionId < 1 || nextTransactionNo < 1) {
            throw new TransactionStateException("restore counters must be >= 1: id=" + nextTransactionId
                    + " no=" + nextTransactionNo);
        }
        lock.lock();
        try {
            if (nextTransactionId > this.nextTransactionId) {
                this.nextTransactionId = nextTransactionId;
            }
            if (nextTransactionNo > this.nextTransactionNo) {
                this.nextTransactionNo = nextTransactionNo;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 原子捕获两个 next-counter，供 fuzzy checkpoint sidecar 使用。快照不消费号码，也不复制活跃事务表；
     * 锁内只读两个 long 后立即释放，不执行文件 IO。
     *
     * @return 下一事务 id/no 的不可变高水位快照。
     */
    public TransactionCounterSnapshot snapshotCounters() {
        lock.lock();
        try {
            return new TransactionCounterSnapshot(
                    TransactionId.of(nextTransactionId), TransactionNo.of(nextTransactionNo));
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
            // lowLimitNo = 当前下一个待分配 TransactionNo：此刻之前提交的事务其 TransactionNo 必 < 它。
            ReadView view = new ReadView(txn.transactionId(), up, low, ids, nextTransactionNo);
            liveReadViews.add(view);
            return view;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 注销一个存活 ReadView（purge 低水位用）。RR 由 {@link ReadViewManager#release} 在事务终态前调用，
     * RC 由调用方语句末经 {@link ReadViewManager#closeReadView} 调用。幂等：重复/未登记的 view 注销为 no-op。
     *
     * @param view 待注销的一致性读快照，不能为 null。
     */
    void closeReadView(ReadView view) {
        if (view == null) {
            throw new TransactionStateException("closeReadView view must not be null");
        }
        lock.lock();
        try {
            liveReadViews.remove(view);
        } finally {
            lock.unlock();
        }
    }

    /**
     * purge 提交序低水位（设计 §7.7、§5.6；mvcc §5.4）：所有存活 ReadView 的 {@code min(lowLimitNo)}；无存活
     * ReadView 则为当前 {@code nextTransactionNo}（此刻之前提交的全部 undo 都可 purge）。一条已提交 undo log 的
     * {@code TransactionNo} 严格小于本返回值即可被 purge 物理回收——它在每个存活快照创建前就已提交、对所有快照可见。
     *
     * <p>锁内读 + 拷贝即返回，不访问 Buffer Pool、不等待（§17 锁顺序约束）。
     *
     * @return purge 边界（TransactionNo）。
     */
    public TransactionNo purgeLowWaterNo() {
        lock.lock();
        try {
            long low = nextTransactionNo;
            for (ReadView v : liveReadViews) {
                if (v.lowLimitNo() < low) {
                    low = v.lowLimitNo();
                }
            }
            return TransactionNo.of(low);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 对一个 history head 做最终 purge 安全复核。仅比较 {@code commitNo < lowLimitNo} 不足以覆盖“提交号已分配、
     * 物理 commit 尚未落盘”窗口：此时新 ReadView 的 lowLimitNo 可能已越过该号，但 creator 仍在 activeIds 中。
     * 因而本方法在同一短锁内同时要求 creator 已从 active table 移除、提交号低于所有快照边界，且 creator
     * 对每个存活 ReadView 均已可见。active 检查不可省略：live 集合为空时，单靠“对所有快照可见”会真空成立。
     *
     * <p>锁内只读不可变 ReadView 和计数器，不执行 IO；purge 在调用后仍须通过 history lease 串行化物理 unlink。
     */
    public boolean isPurgeEligible(TransactionNo commitNo, TransactionId creatorTransactionId) {
        if (commitNo == null || commitNo.isNone() || creatorTransactionId == null
                || creatorTransactionId.isNone()) {
            throw new TransactionStateException("purge eligibility requires assigned transaction no/id");
        }
        lock.lock();
        try {
            if (active.contains(creatorTransactionId.value())) {
                return false;
            }
            long low = nextTransactionNo;
            for (ReadView view : liveReadViews) {
                if (view.lowLimitNo() < low) {
                    low = view.lowLimitNo();
                }
                if (!view.isVisible(creatorTransactionId)) {
                    return false;
                }
            }
            return commitNo.value() < low;
        } finally {
            lock.unlock();
        }
    }
}
