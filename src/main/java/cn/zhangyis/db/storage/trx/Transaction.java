package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

/**
 * 数据库事务聚合（innodb-transaction-mvcc-design §5.2）。单 owner 线程使用（一个 session 一个事务）。
 *
 * <p>状态、事务 id、提交序号只由 {@link TransactionManager} 经状态机修改（包内可见 setter），不暴露公共 setter，
 * 避免外部绕过状态机破坏不变量。本片不持有 {@code BufferFrame}/{@code PageGuard}，也不含 readView 字段
 * （ReadView 推迟到可见性片）。
 *
 * <p>T1.3c 起，事务聚合挂一个惰性 {@link UndoContext}（事务运行时 undo 子状态），由 {@code UndoLogManager} 在
 * 首次普通 undo 写时经包内可见 {@link #setUndoContext} 绑定；此前为 {@code null}，表示尚未建任何 undo log。
 * {@code UndoContext} 的内部推进（事务全局 {@code lastUndoNo} 与 INSERT/UPDATE 两个局部 head）由 {@code UndoLogManager} 调其
 * 包内 setter 完成，不经本类。
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
    /**
     * ACTIVE 事务的提交资格：statement rollback 出现结果不确定错误后置为 true。它不替代主状态机——事务仍留在
     * active table，唯一合法收尾是完整 rollback；后续 DML、prepareCommit 和 commit 必须拒绝。
     */
    private boolean rollbackOnly;
    /** 首次把事务置为 rollback-only 的领域原因，供拒绝提交和恢复诊断；正常事务为空字符串。 */
    private String rollbackOnlyReason = "";
    /** 事务 undo 子状态：惰性绑定，首写前为 {@code null}（未建任何 INSERT/UPDATE log）。仅 UndoLogManager 修改。 */
    private UndoContext undoContext;
    /**
     * 一致性读快照（T1.4）：RR 事务级复用——首次一致性读经 {@code ReadViewManager.openReadView} 绑定，
     * 整事务复用同一对象，commit/rollback 时 {@code release} 清空；RC 每读新建、不在此缓存。仅 ReadViewManager 修改。
     */
    private ReadView readView;

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

    /** 当前 ACTIVE 事务是否已经失去提交资格，只能执行完整 rollback。 */
    public boolean rollbackOnly() {
        return rollbackOnly;
    }

    /** 首次触发 rollback-only 的诊断原因；未触发时为空字符串。 */
    public String rollbackOnlyReason() {
        return rollbackOnlyReason;
    }

    /** 事务 undo 子状态；首写前为 {@code null}（未建任何普通 undo log）。 */
    public UndoContext undoContext() {
        return undoContext;
    }

    /** 事务级一致性读快照（RR 复用）；未开或已 release 时为 {@code null}。 */
    public ReadView readView() {
        return readView;
    }

    // ---- 包内可见：仅 TransactionManager / UndoLogManager 调用 ----

    void setTransactionId(TransactionId id) {
        this.transactionId = id;
    }

    void setTransactionNo(TransactionNo no) {
        this.transactionNo = no;
    }

    /**
     * 绑定事务 undo 子状态。由 {@code UndoLogManager.appendPlanned} 在首条任意 kind undo 成功后调用；Java null 引用必须拒绝
     * （避免隐藏 NPE），但调用方控制单次绑定，本 mutator 不强制单次以保持生命周期约束集中在 manager。
     *
     * @param ctx undo 子状态，不能为 null。
     */
    void setUndoContext(UndoContext ctx) {
        if (ctx == null) {
            throw new DatabaseValidationException("undo context must not be null");
        }
        this.undoContext = ctx;
    }

    /**
     * 绑定事务级一致性读快照（RR）。仅 {@code ReadViewManager.openReadView} 调用；不能为 null。
     */
    void bindReadView(ReadView view) {
        if (view == null) {
            throw new DatabaseValidationException("read view must not be null");
        }
        this.readView = view;
    }

    /** 清空事务级 ReadView（RR）。仅 {@code ReadViewManager.release} 调用；幂等（已空再清无副作用）。 */
    void clearReadView() {
        this.readView = null;
    }

    /**
     * 把 ACTIVE 事务标记为只能完整回滚。重复调用保留首次原因，避免后续清理异常覆盖最初的数据一致性故障。
     * 主状态仍保持 ACTIVE，使事务继续留在活跃表并能被普通/恢复 rollback 消费 undo。
     */
    void markRollbackOnly(String reason) {
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("rollback-only reason must not be blank");
        }
        if (!rollbackOnly) {
            rollbackOnly = true;
            rollbackOnlyReason = reason;
        }
    }

    /** 经状态机校验后推进状态；非法转换抛 {@link TransactionStateException}。 */
    void transitionTo(TransactionState target) {
        if (!state.canTransitionTo(target)) {
            throw new TransactionStateException("illegal transition " + state + " -> " + target);
        }
        this.state = target;
    }
}
