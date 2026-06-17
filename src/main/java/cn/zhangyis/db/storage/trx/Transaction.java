package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

/**
 * 数据库事务聚合（innodb-transaction-mvcc-design §5.2）。单 owner 线程使用（一个 session 一个事务）。
 *
 * <p>状态、事务 id、提交序号只由 {@link TransactionManager} 经状态机修改（包内可见 setter），不暴露公共 setter，
 * 避免外部绕过状态机破坏不变量。本片不持有 {@code BufferFrame}/{@code PageGuard}，也不含 readView 字段
 * （ReadView 推迟到可见性片）。
 */
public final class Transaction {

    /** 隔离级别（本片仅记录，不驱动行为）。 */
    private final IsolationLevel isolationLevel;
    /** 只读标志：只读事务不分配写 id、不进活跃表、commit 不分配 transactionNo。 */
    private final boolean readOnly;
    /** 自动提交标志（本片仅记录）。 */
    private final boolean autoCommit;
    /** 事务开始时间（epoch millis，诊断用）。 */
    private final long startTimeMillis;
    /** 写者标识：惰性分配，只读/未写时为 {@link TransactionId#NONE}。 */
    private TransactionId transactionId = TransactionId.NONE;
    /** 提交序号：commit 给读写事务分配；之前为 {@link TransactionNo#NONE}。 */
    private TransactionNo transactionNo = TransactionNo.NONE;
    /** 事务状态：仅经 {@link #transitionTo} 推进。 */
    private TransactionState state = TransactionState.ACTIVE;

    Transaction(TransactionOptions options, long startTimeMillis) {
        this.isolationLevel = options.isolationLevel();
        this.readOnly = options.readOnly();
        this.autoCommit = options.autoCommit();
        this.startTimeMillis = startTimeMillis;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public boolean readOnly() {
        return readOnly;
    }

    public boolean autoCommit() {
        return autoCommit;
    }

    public long startTimeMillis() {
        return startTimeMillis;
    }

    public TransactionId transactionId() {
        return transactionId;
    }

    public TransactionNo transactionNo() {
        return transactionNo;
    }

    public TransactionState state() {
        return state;
    }

    // ---- 包内可见：仅 TransactionManager 调用 ----

    void setTransactionId(TransactionId id) {
        this.transactionId = id;
    }

    void setTransactionNo(TransactionNo no) {
        this.transactionNo = no;
    }

    /** 经状态机校验后推进状态；非法转换抛 {@link TransactionStateException}。 */
    void transitionTo(TransactionState target) {
        if (!state.canTransitionTo(target)) {
            throw new TransactionStateException("illegal transition " + state + " -> " + target);
        }
        this.state = target;
    }
}
