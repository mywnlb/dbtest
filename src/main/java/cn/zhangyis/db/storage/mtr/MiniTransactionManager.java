package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.redo.RedoCapacityThrottle;
import cn.zhangyis.db.storage.redo.RedoAppendBudget;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;

import java.util.concurrent.atomic.AtomicLong;

/**
 * mini-transaction 管理器（设计 §13.2）：把 MTR 绑定到当前线程，禁止静默嵌套。
 * begin/commit/rollback 必须在同一线程；commit/rollback 用 try/finally 保证解绑，即使释放抛异常也不泄漏绑定。
 */
public final class MiniTransactionManager {

    /** 所有本 Manager 创建的 MTR 共享；截断服务必须注入同一实例形成真实互斥。 */
    private final TablespaceAccessController accessController;

    /** 当前线程绑定的 MTR；天然按线程隔离。 */
    private final ThreadLocal<MiniTransaction> current = new ThreadLocal<>();

    /** MTR id 分配器。 */
    private final AtomicLong idSequence = new AtomicLong();

    /** 全局 redo 日志管理器（D3 内存版）；注入每个 MTR，测试经 {@link #redoLogManager()} 检视。 */
    private final RedoLogManager redoLogManager;

    /** redo 预算申请前的容量反压；默认 no-op，生产引擎可注入真实 throttle。 */
    private final RedoCapacityThrottle redoCapacityThrottle;
    /** 实例页大小感知的生产操作 profile；只估算，不读取或修改 MTR/redo 状态。 */
    private final MtrOperationRedoBudgetEstimator operationBudgetEstimator;
    public MiniTransactionManager() {
        this(new TablespaceAccessController(), new RedoLogManager());
    }

    public MiniTransactionManager(TablespaceAccessController accessController) {
        this(accessController, new RedoLogManager());
    }

    /**
     * 创建共享 operation lease 与指定 redo manager 的 MTR 管理器。截断 marker 必须使用 durable redo manager，
     * 从而让 commit 返回的 end LSN 能在物理缩短前 fsync。
     */
    public MiniTransactionManager(TablespaceAccessController accessController, RedoLogManager redoLogManager) {
        this(accessController, redoLogManager, RedoCapacityThrottle.NO_OP);
    }

    /**
     * 创建带 redo capacity throttle 的 MTR 管理器。三参构造不申请额外预算，只保留 begin-time 当前压力检查；
     * 生产引擎应使用四参构造传入前台预算，避免多个 MTR 在低水位 begin 后集中 append。
     */
    public MiniTransactionManager(TablespaceAccessController accessController,
                                  RedoLogManager redoLogManager,
                                  RedoCapacityThrottle redoCapacityThrottle) {
        this(accessController, redoLogManager, redoCapacityThrottle, PageSize.ofBytes(16 * 1024));
    }

    /** 创建使用实例真实页大小计算操作级 redo 上界的生产 MTR 管理器。 */
    public MiniTransactionManager(TablespaceAccessController accessController,
                                  RedoLogManager redoLogManager,
                                  RedoCapacityThrottle redoCapacityThrottle,
                                  PageSize pageSize) {
        if (accessController == null || redoLogManager == null || redoCapacityThrottle == null) {
            throw new DatabaseValidationException("MTR access controller/redo manager/throttle must not be null");
        }
        this.accessController = accessController;
        this.redoLogManager = redoLogManager;
        this.redoCapacityThrottle = redoCapacityThrottle;
        this.operationBudgetEstimator = new MtrOperationRedoBudgetEstimator(pageSize);
    }

    /**
     * 开启并绑定一个 MTR。已有当前 MTR 则抛异常（禁静默嵌套，需嵌套应显式建 child）。
     *
     * @return 已 ACTIVE 的 MTR。
     */
    public MiniTransaction begin() {
        if (redoCapacityThrottle.requiresExplicitBudget()) {
            throw new MtrStateException("capacity-aware mini transaction requires an explicit redo budget");
        }
        return beginInternal(RedoAppendBudget.testingUnbounded());
    }

    /** 开启显式零预算的只读 MTR；即使 redo 接近水位也不触发 flush/checkpoint。 */
    public MiniTransaction beginReadOnly() {
        return beginInternal(RedoAppendBudget.readOnly());
    }

    /**
     * 按操作级上界开启写 MTR。预算准入先于 MTR 暴露给调用方，因而调用方尚不可能持有本 MTR 的页/FSP 资源。
     */
    public MiniTransaction begin(RedoAppendBudget budget) {
        if (budget == null) {
            throw new DatabaseValidationException("mini transaction redo budget must not be null");
        }
        return beginInternal(budget);
    }

    /**
     * 返回实例页大小感知的写操作上界。调用方必须在取得 page/FSP 资源前计算并立即传给 {@link #begin(RedoAppendBudget)}。
     */
    public RedoAppendBudget budgetFor(RedoBudgetPurpose purpose) {
        return operationBudgetEstimator.estimate(purpose);
    }

    /** 把 begin 前由 BTree/Undo/DML 物化的领域 workload 转为实例级 redo admission 上界。 */
    public RedoAppendBudget budgetFor(RedoBudgetPurpose purpose, RedoBudgetWorkload workload) {
        return operationBudgetEstimator.estimate(purpose, workload);
    }

    private MiniTransaction beginInternal(RedoAppendBudget budget) {
        if (current.get() != null) {
            throw new MtrStateException("nested mini transaction not allowed on this thread; create an explicit child");
        }
        // redo capacity reservation 必须发生在 MTR 取得任何 page latch、buffer fix 或 tablespace lease 之前。
        // 若在 commit 阶段等待，调用方通常已持有页锁与 FIL/FSP lease，会把 redo/checkpoint 压力放大成页锁等待链。
        RedoCapacityThrottle.Reservation reservation =
                redoCapacityThrottle.reserveAppendBudget(budget);
        try {
            MiniTransaction mtr = new MiniTransaction(idSequence.incrementAndGet(), redoLogManager,
                    accessController, budget, reservation);
            mtr.activate();
            mtr.enlistResource(reservation);
            current.set(mtr);
            return mtr;
        } catch (RuntimeException e) {
            reservation.close();
            throw e;
        }
    }

    /** 本管理器的 redo 日志管理器（D3 内存版）。 */
    public RedoLogManager redoLogManager() {
        return redoLogManager;
    }

    /**
     * 返回当前线程绑定的 MTR；无则抛异常。
     *
     * @return 当前 MTR。
     */
    public MiniTransaction current() {
        MiniTransaction mtr = current.get();
        if (mtr == null) {
            throw new MtrStateException("no active mini transaction on this thread");
        }
        return mtr;
    }

    /**
     * 提交并解绑。mtr 必须是当前线程绑定的那个；释放资源后无论成败都解绑。
     *
     * @param mtr 待提交 MTR。
     */
    public cn.zhangyis.db.domain.Lsn commit(MiniTransaction mtr) {
        requireBound(mtr);
        try {
            return mtr.commit();
        } finally {
            current.remove();
        }
    }

    /**
     * 回滚未提交 MTR 并解绑（不撤销 buffer 改动）。
     *
     * @param mtr 待回滚 MTR。
     */
    public void rollbackUncommitted(MiniTransaction mtr) {
        requireBound(mtr);
        try {
            mtr.rollbackUncommitted();
        } finally {
            current.remove();
        }
    }

    private void requireBound(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (current.get() != mtr) {
            throw new MtrStateException("mini transaction not bound to current thread");
        }
    }
}
