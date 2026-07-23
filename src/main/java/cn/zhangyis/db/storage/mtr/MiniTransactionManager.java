package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.redo.RedoCapacityThrottle;
import cn.zhangyis.db.storage.redo.RedoAppendBudget;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.engine.StorageWriteAdmission;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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
    /** 所有写 MTR 在 redo 预算与页资源之前检查的实例级准入；只读 MTR 不消费该能力。 */
    private final StorageWriteAdmission writeAdmission;
    /**
     * 创建 {@code MiniTransactionManager}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     */
    public MiniTransactionManager() {
        this(new TablespaceAccessController(), new RedoLogManager());
    }

    /**
     * 创建 {@code MiniTransactionManager}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param accessController 由组合根提供的 {@code TablespaceAccessController} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     */
    public MiniTransactionManager(TablespaceAccessController accessController) {
        this(accessController, new RedoLogManager());
    }

    /**
     * 创建共享 operation lease 与指定 redo manager 的 MTR 管理器。截断 marker 必须使用 durable redo manager，
     * 从而让 commit 返回的 end LSN 能在物理缩短前 fsync。
     *
     * @param accessController 由组合根提供的 {@code TablespaceAccessController} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redoLogManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     */
    public MiniTransactionManager(TablespaceAccessController accessController, RedoLogManager redoLogManager) {
        this(accessController, redoLogManager, RedoCapacityThrottle.NO_OP);
    }

    /**
     * 创建带 redo capacity throttle 的 MTR 管理器。三参构造不申请额外预算，只保留 begin-time 当前压力检查；
     * 生产引擎应使用四参构造传入前台预算，避免多个 MTR 在低水位 begin 后集中 append。
     *
     * @param accessController 由组合根提供的 {@code TablespaceAccessController} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redoLogManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param redoCapacityThrottle 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     */
    public MiniTransactionManager(TablespaceAccessController accessController,
                                  RedoLogManager redoLogManager,
                                  RedoCapacityThrottle redoCapacityThrottle) {
        this(accessController, redoLogManager, redoCapacityThrottle, PageSize.ofBytes(16 * 1024));
    }

    /** 创建使用实例真实页大小计算操作级 redo 上界的生产 MTR 管理器。
     *
     * @param accessController 由组合根提供的 {@code TablespaceAccessController} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redoLogManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param redoCapacityThrottle 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public MiniTransactionManager(TablespaceAccessController accessController,
                                  RedoLogManager redoLogManager,
                                  RedoCapacityThrottle redoCapacityThrottle,
                                  PageSize pageSize) {
        this(accessController, redoLogManager, redoCapacityThrottle, pageSize,
                StorageWriteAdmission.normal());
    }

    /**
     * 创建带实例级写闸门的生产 MTR 管理器。
     *
     * @param accessController 表空间 operation lease owner
     * @param redoLogManager 当前实例 redo owner
     * @param redoCapacityThrottle begin-time redo 预算准入
     * @param pageSize 实例固定页大小
     * @param writeAdmission 在任何写 MTR 资源获取前检查的组合根闸门
     */
    public MiniTransactionManager(TablespaceAccessController accessController,
                                  RedoLogManager redoLogManager,
                                  RedoCapacityThrottle redoCapacityThrottle,
                                  PageSize pageSize,
                                  StorageWriteAdmission writeAdmission) {
        if (accessController == null || redoLogManager == null || redoCapacityThrottle == null) {
            throw new DatabaseValidationException("MTR access controller/redo manager/throttle must not be null");
        }
        if (writeAdmission == null) {
            throw new DatabaseValidationException("MTR storage write admission must not be null");
        }
        this.accessController = accessController;
        this.redoLogManager = redoLogManager;
        this.redoCapacityThrottle = redoCapacityThrottle;
        this.operationBudgetEstimator = new MtrOperationRedoBudgetEstimator(pageSize);
        this.writeAdmission = writeAdmission;
    }

    /**
     * 开启并绑定一个 MTR。已有当前 MTR 则抛异常（禁静默嵌套，需嵌套应显式建 child）。
     *
     * @return 已 ACTIVE 的 MTR。
     * @throws MtrStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public MiniTransaction begin() {
        writeAdmission.assertWriteAllowed();
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
     *
     * @param budget redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code begin} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public MiniTransaction begin(RedoAppendBudget budget) {
        if (budget == null) {
            throw new DatabaseValidationException("mini transaction redo budget must not be null");
        }
        writeAdmission.assertWriteAllowed();
        return beginInternal(budget);
    }

    /**
     * 返回实例页大小感知的写操作上界。调用方必须在取得 page/FSP 资源前计算并立即传给 {@link #begin(RedoAppendBudget)}。
     * @param purpose 选择 {@code budgetFor} 分支的 {@code RedoBudgetPurpose} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code budgetFor} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public RedoAppendBudget budgetFor(RedoBudgetPurpose purpose) {
        return operationBudgetEstimator.estimate(purpose);
    }

    /** 把 begin 前由 BTree/Undo/DML 物化的领域 workload 转为实例级 redo admission 上界。
     *
     * @param purpose 选择 {@code budgetFor} 分支的 {@code RedoBudgetPurpose} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param workload redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code budgetFor} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public RedoAppendBudget budgetFor(RedoBudgetPurpose purpose, RedoBudgetWorkload workload) {
        return operationBudgetEstimator.estimate(purpose, workload);
    }

    /**
     * 在当前线程可能已绑定父 MTR 的情况下执行一个显式独立提交的物理子操作。该能力只供页面发布前 merge：
     * 父 MTR 保持其既有 latch/lease，但暂时从 ThreadLocal 隐藏；子 MTR 使用自己的 redo range、memo 与提交边界，
     * 完成后无条件恢复父绑定。普通业务代码仍应使用 begin/commit，不能借此制造任意嵌套事务。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>保存当前父绑定并临时移除，使 begin 的显式预算准入发生在子 MTR 获取资源之前。</li>
     *     <li>创建子 MTR 并执行回调；回调只能操作与父持有资源具有书面无环证明的发布前页面/global ibuf 页。</li>
     *     <li>正常返回时提交独立 redo；回调在 ACTIVE 阶段失败则释放子 memo，提交期失败保持 fail-stop 状态。</li>
     *     <li>finally 清除任何子绑定并恢复原父 MTR，确保外层调用随后继续完成原 getPage/fix 生命周期。</li>
     * </ol>
     *
     * @param budget 覆盖整个子操作最坏分支的显式 redo 上界；不得为 {@code null}
     * @param work 在新子 MTR 内执行并返回稳定结果的回调；不得为 {@code null}
     * @param <T> 回调返回值类型
     * @return 子 MTR 成功提交后返回的回调结果
     * @throws DatabaseValidationException 参数为空时抛出
     */
    public <T> T executeDetached(RedoAppendBudget budget, Function<MiniTransaction, T> work) {
        // 1、父绑定只暂存于当前栈帧，不向其它线程发布。
        if (budget == null || work == null) {
            throw new DatabaseValidationException("detached mini transaction budget/work must not be null");
        }
        MiniTransaction parent = current.get();
        current.remove();
        MiniTransaction child = null;
        try {
            // 2、子 MTR 走完整 write-admission 和 redo capacity reservation。
            child = begin(budget);
            T result = work.apply(child);
            // 3、只有独立 redo/pageLSN/dirty 发布完整完成才向发布前拦截器返回。
            commit(child);
            return result;
        } catch (RuntimeException failure) {
            if (child != null && child.state() == MiniTransactionState.ACTIVE && current.get() == child) {
                try {
                    rollbackUncommitted(child);
                } catch (RuntimeException releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            throw failure;
        } finally {
            // 4、commit/rollback 均应已解绑；防御性清理后恢复外层 MTR identity。
            current.remove();
            if (parent != null) {
                current.set(parent);
            }
        }
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
     * @throws MtrStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
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
     * @return {@code commit} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
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
