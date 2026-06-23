package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
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
        if (accessController == null || redoLogManager == null) {
            throw new DatabaseValidationException("MTR access controller/redo manager must not be null");
        }
        this.accessController = accessController;
        this.redoLogManager = redoLogManager;
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
        MiniTransaction mtr = new MiniTransaction(idSequence.incrementAndGet(), redoLogManager, accessController);
        mtr.activate();
        current.set(mtr);
        return mtr;
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
