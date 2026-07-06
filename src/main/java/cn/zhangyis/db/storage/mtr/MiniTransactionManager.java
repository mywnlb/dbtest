package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.RedoCapacityThrottle;
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
    /**
     * 每个前台 MTR 在 begin 时申请的 redo 预算。该预算只参与 capacity log-free-check，不分配真实 LSN；
     * 真实 LSN 仍在 commit append 时按实际 record 长度分配。生产引擎按 redo capacity 配置保守估算，测试默认 0。
     */
    private final long foregroundRedoReservationBytes;

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
        this(accessController, redoLogManager, redoCapacityThrottle, 0);
    }

    /**
     * 创建带前台 redo 预算的 MTR 管理器。预算在 {@link #begin()} 时申请并挂入 MTR memo，保证容量等待发生在
     * page latch、buffer fix 和 tablespace lease 之前；commit 只做真实 append 与 pageLSN 盖戳，不再等待 checkpoint。
     */
    public MiniTransactionManager(TablespaceAccessController accessController,
                                  RedoLogManager redoLogManager,
                                  RedoCapacityThrottle redoCapacityThrottle,
                                  long foregroundRedoReservationBytes) {
        if (accessController == null || redoLogManager == null || redoCapacityThrottle == null) {
            throw new DatabaseValidationException("MTR access controller/redo manager/throttle must not be null");
        }
        if (foregroundRedoReservationBytes < 0) {
            throw new DatabaseValidationException("foreground redo reservation bytes must not be negative: "
                    + foregroundRedoReservationBytes);
        }
        this.accessController = accessController;
        this.redoLogManager = redoLogManager;
        this.redoCapacityThrottle = redoCapacityThrottle;
        this.foregroundRedoReservationBytes = foregroundRedoReservationBytes;
    }

    /**
     * 开启并绑定一个 MTR。已有当前 MTR 则抛异常（禁静默嵌套，需嵌套应显式建 child）。
     *
     * @return 已 ACTIVE 的 MTR。
     */
    public MiniTransaction begin() {
        if (current.get() != null) {
            throw new MtrStateException("nested mini transaction not allowed on this thread; create an explicit child");
        }
        // redo capacity reservation 必须发生在 MTR 取得任何 page latch、buffer fix 或 tablespace lease 之前。
        // 若在 commit 阶段等待，调用方通常已持有页锁与 FIL/FSP lease，会把 redo/checkpoint 压力放大成页锁等待链。
        RedoCapacityThrottle.Reservation reservation =
                redoCapacityThrottle.reserveAppendBytes(foregroundRedoReservationBytes);
        try {
            MiniTransaction mtr = new MiniTransaction(idSequence.incrementAndGet(), redoLogManager,
                    accessController);
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
