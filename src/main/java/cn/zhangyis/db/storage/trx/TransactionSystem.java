package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ReadViewRetentionInterruptedException;
import cn.zhangyis.db.storage.api.ReadViewRetentionTimeoutException;

import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务全局协调器（innodb-transaction-mvcc-design §5.3、§17）。
 *
 * <p>并发：用一把 {@link ReentrantLock} 短锁保护 {@code nextTransactionId}/{@code nextTransactionNo} 与私有
 * {@link ActiveTransactionTable}。所有分配/登记/移除/快照均在锁内完成，{@link #snapshotActiveReadWriteIds()}
 * 拷贝后立即释放锁。**持锁期间不访问 Buffer Pool、不持 page latch、不等待**（§17 锁顺序约束），故该锁不会卷入
 * 任何可阻塞路径。禁止 {@code synchronized}。
 */
public final class TransactionSystem implements cn.zhangyis.db.storage.api.ReadViewRetentionBarrier {

    /** 保护下列三项全局状态的短锁。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** ReadView关闭时唤醒SHADOW finalization；只与{@link #lock}配合，不参与页或事务锁等待。 */
    private final Condition readViewClosed = lock.newCondition();
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
    private final Map<ReadView, Long> liveReadViews = new IdentityHashMap<>();
    /** 下一个ReadView generation；generation 0表示尚未创建快照。 */
    private long nextReadViewGeneration = 1;

    /** 分配单调事务写 id 并登记为活跃读写事务（首次写入时调用）。
     *
     * @return {@code allocateWriteId} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
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

    /** 分配单调提交序号（commit 时给读写事务调用）。
     *
     * @return {@code allocateTransactionNo} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    TransactionNo allocateTransactionNo() {
        lock.lock();
        try {
            return TransactionNo.of(nextTransactionNo++);
        } finally {
            lock.unlock();
        }
    }

    /** 从活跃表移出（commit/rollback 读写事务）。
     *
     * @param txnId 参与 {@code removeActive} 的原始数值身份 {@code txnId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
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
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
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

    /** 活跃读写事务 id 的不可变快照（拷贝后立即释放锁）。
     *
     * @return 调用时刻的不可变状态集合或映射；没有已发布条目时返回空集合，调用方修改不会影响权威状态
     */
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
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
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
            if (nextReadViewGeneration == Long.MAX_VALUE) {
                throw new DatabaseFatalException("ReadView generation exhausted");
            }
            liveReadViews.put(view, nextReadViewGeneration++);
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
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    void closeReadView(ReadView view) {
        if (view == null) {
            throw new TransactionStateException("closeReadView view must not be null");
        }
        lock.lock();
        try {
            if (liveReadViews.remove(view) != null) {
                readViewClosed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 捕获调用时刻最后一个已登记ReadView的generation，供table X下冻结SHADOW cutover边界。
     * 本操作不创建快照、不等待且不访问page/undo；返回0表示实例尚未产生ReadView。
     *
     * @return 非负generation fence，之后新建的ReadView不会被该fence等待
     */
    public long captureReadViewGeneration() {
        lock.lock();
        try {
            return nextReadViewGeneration - 1;
        } finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public long captureGeneration() {
        return captureReadViewGeneration();
    }

    /**
     * 有界等待generation不晚于fence的全部ReadView退出。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>把timeout转换为单一绝对deadline，并在该预算内取得事务系统短锁。</li>
     *     <li>扫描identity map中的generation；只要存在旧快照就在Condition上释放锁等待。</li>
     *     <li>每次唤醒重新扫描以容忍虚假唤醒；超时或中断均释放锁且不修改ReadView集合。</li>
     * </ol>
     *
     * @param generationFence final table X下捕获的非负generation；0表示无需等待历史快照
     * @param timeout 取得协调锁及全部Condition等待共用的正预算
     * @throws DatabaseValidationException fence或timeout非法时抛出，不进入等待
     * @throws ReadViewRetentionTimeoutException 预算耗尽仍有旧快照时抛出，调用方可恢复capture重试
     * @throws ReadViewRetentionInterruptedException 等待被中断时抛出并恢复线程中断标志
     */
    public void awaitReadViewsClosedThrough(long generationFence, Duration timeout) {
        // 1. 一个绝对deadline覆盖短锁竞争和全部Condition等待，不能在虚假唤醒后重置预算。
        if (generationFence < 0 || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "ReadView retention requires non-negative fence and positive timeout");
        }
        if (generationFence == 0) {
            return;
        }
        long budgetNanos = boundedTimeoutNanos(timeout);
        long deadline = saturatedDeadline(budgetNanos);
        try {
            if (!lock.tryLock(budgetNanos, TimeUnit.NANOSECONDS)) {
                throw new ReadViewRetentionTimeoutException(
                        "timed out acquiring ReadView retention lock: fence=" + generationFence);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new ReadViewRetentionInterruptedException(
                    "interrupted acquiring ReadView retention lock: fence=" + generationFence,
                    interrupted);
        }
        try {
            // 2. Condition等待原子释放短锁，ReadView close可取得同一锁并signal；期间不持任何page资源。
            while (hasReadViewAtOrBefore(generationFence)) {
                long remaining = remainingNanos(deadline);
                if (remaining <= 0) {
                    throw new ReadViewRetentionTimeoutException(
                            "timed out waiting old ReadViews: fence=" + generationFence);
                }
                try {
                    readViewClosed.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new ReadViewRetentionInterruptedException(
                            "interrupted waiting old ReadViews: fence=" + generationFence,
                            interrupted);
                }
            }
            // 3. 正常返回证明fence前集合为空；finally统一释放协调锁。
        } finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void awaitClosedThrough(long generationFence, Duration timeout) {
        awaitReadViewsClosedThrough(generationFence, timeout);
    }

    /** 只在事务系统锁内扫描有界live集合，不执行IO或调用外部协作者。 */
    private boolean hasReadViewAtOrBefore(long generationFence) {
        for (long generation : liveReadViews.values()) {
            if (generation <= generationFence) {
                return true;
            }
        }
        return false;
    }

    /** 超大正Duration饱和到long纳秒上界。 */
    private static long boundedTimeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /** 单调时钟加法饱和，避免deadline回绕成已超时。 */
    private static long saturatedDeadline(long budgetNanos) {
        long now = System.nanoTime();
        if (budgetNanos == Long.MAX_VALUE || Long.MAX_VALUE - now < budgetNanos) {
            return Long.MAX_VALUE;
        }
        return now + budgetNanos;
    }

    /** Long.MAX_VALUE deadline保持近似无界，其余使用单调时钟计算剩余预算。 */
    private static long remainingNanos(long deadline) {
        return deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
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
            for (ReadView v : liveReadViews.keySet()) {
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
     * @param commitNo 参与 {@code isPurgeEligible} 的稳定领域标识 {@code TransactionNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param creatorTransactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @return {@code isPurgeEligible} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
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
            for (ReadView view : liveReadViews.keySet()) {
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
