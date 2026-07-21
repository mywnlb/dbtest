package cn.zhangyis.db.engine.xa;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.XaId;
import cn.zhangyis.db.session.xa.SessionXaCoordinator;
import cn.zhangyis.db.session.xa.XaRecoverEntry;
import cn.zhangyis.db.sql.executor.storage.SqlStorageGateway;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DatabaseEngine 共享 XA coordinator。全局 map 锁只保护 branch 注册和移除；每个 branch 的公平锁
 * 串行 prepare/phase-two，持有 branch 锁执行 IO 不会阻塞其它 XID，也不与 storage 内部锁互相获取。
 */
public final class PersistentXaCoordinator implements SessionXaCoordinator {

    /** XID 权威持久目录。 */
    private final FileXaRegistry registry;
    /** 只保护 preparedBranches map 的短临界区。 */
    private final ReentrantLock branchesLock = new ReentrantLock(true);
    /** 当前进程完成 phase one、仍可执行 phase two 的 opaque branch。 */
    private final Map<XaId, PreparedBranch> preparedBranches = new HashMap<>();

    /**
     * 创建共享 coordinator。
     *
     * @param registry 已在 instance lock 下打开并完成恢复扫描的 registry
     */
    public PersistentXaCoordinator(FileXaRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("persistent XA coordinator registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 执行 phase one 三段式持久协议。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>短锁下为 XID 注册唯一运行期 branch，拒绝其它 handle 或未完成旧 XID。</li>
     *     <li>释放 map 锁后 fsync PREPARING，使 XID→transactionId 先于 storage PREPARED 可恢复。</li>
     *     <li>调用 opaque gateway 强持久 storage PREPARED；失败保留 PREPARING 与 branch，不猜测成功。</li>
     *     <li>fsync PREPARED 后发布 branch PREPARED；只有此后 Session 才可释放活动 metadata scope。</li>
     * </ol>
     *
     * @param xid 待 prepare 分支
     * @param transactionId gateway 已确认的正 write transaction id
     * @param gateway opaque transaction 的 owner adapter
     * @param handle 仍为 ACTIVE 的 write handle
     * @param timeout phase-one 正等待上限
     */
    @Override
    public void prepare(XaId xid, long transactionId, SqlStorageGateway gateway,
                        SqlTransactionHandle handle, Duration timeout) {
        validate(xid, transactionId, gateway, handle, timeout);
        WaitDeadline deadline = WaitDeadline.after(timeout);
        // 1、map 锁不跨 registry/storage IO；同 XID 的实际工作由 branch lock 串行。
        PreparedBranch branch;
        branchesLock.lock();
        try {
            branch = preparedBranches.get(xid);
            if (branch == null) {
                branch = new PreparedBranch(xid, TransactionId.of(transactionId), gateway, handle);
                preparedBranches.put(xid, branch);
            } else if (branch.transactionId.value() != transactionId
                    || branch.gateway != gateway || branch.handle != handle) {
                throw new XaException("XA XID already belongs to another live branch: " + xid);
            }
        } finally {
            branchesLock.unlock();
        }

        lockBranch(branch, deadline, "XA prepare branch owner");
        try {
            if (branch.state == BranchState.PREPARED) {
                return;
            }
            if (branch.state != BranchState.PREPARING) {
                throw new XaException("XA branch cannot prepare from state " + branch.state);
            }
            // 2、若 crash 发生在下一步前，PREPARING 的恢复唯一安全决议是 rollback。
            registry.preparing(xid, branch.transactionId);
            // 3、storage 返回前 PREPARED redo 已 fsync；失败不写伪 PREPARED。
            var outcome = gateway.prepareXa(handle, deadline.remaining("storage XA prepare"));
            if (outcome.transactionId() != transactionId) {
                throw new XaException("XA storage prepare returned a different transaction id");
            }
            // 4、第二个 fsync 是 XA PREPARE 成功响应的最终证据。
            registry.prepared(xid, branch.transactionId);
            branch.state = BranchState.PREPARED;
        } finally {
            branch.operationLock.unlock();
        }
    }

    /** 持久选择 commit 后完成同方向 phase two；任何失败保留 branch 供同命令重试。 */
    @Override
    public void commitPrepared(XaId xid, Duration timeout) {
        complete(xid, timeout, true);
    }

    /** 持久选择 rollback 后完成同方向 phase two；任何失败保留 branch 供同命令重试。 */
    @Override
    public void rollbackPrepared(XaId xid, Duration timeout) {
        complete(xid, timeout, false);
    }

    /**
     * XA RECOVER 只枚举 durable PREPARED；PREPARING/DECIDED 是恢复内部状态，不伪装成 in-doubt。
     */
    @Override
    public List<XaRecoverEntry> recover() {
        return registry.pendingEntries().stream()
                .filter(entry -> entry.state() == XaRegistryState.PREPARED)
                .map(entry -> new XaRecoverEntry(entry.xid(), entry.transactionId().value()))
                .toList();
    }

    /**
     * Phase two 共享实现。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>短锁定位 live prepared branch，随后用 branch 锁串行相反决议与同向重试。</li>
     *     <li>先 fsync 最终决议；一旦成功，后续失败也禁止切换方向。</li>
     *     <li>若 storage 尚未成功，调用对应 prepared gateway 并缓存 terminal 已完成事实。</li>
     *     <li>fsync COMPLETED 后从 live map 移除；map 移除发生在所有持久副作用之后。</li>
     * </ol>
     */
    private void complete(XaId xid, Duration timeout, boolean commit) {
        if (xid == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("XA phase two requires xid and positive timeout");
        }
        WaitDeadline deadline = WaitDeadline.after(timeout);
        // 1、只有本进程完成 phase one 的 opaque handle 可在线 phase two；重启未决必须先离线裁决。
        PreparedBranch branch;
        branchesLock.lock();
        try {
            branch = preparedBranches.get(xid);
        } finally {
            branchesLock.unlock();
        }
        if (branch == null) {
            throw new XaException("XA prepared branch is not live; inspect/decide it offline: " + xid);
        }

        lockBranch(branch, deadline, "XA phase-two branch owner");
        try {
            BranchState decided = commit ? BranchState.COMMIT_DECIDED : BranchState.ROLLBACK_DECIDED;
            if (branch.state == opposite(decided)) {
                throw new XaException("XA branch already has the opposite durable decision: " + xid);
            }
            if (branch.state != BranchState.PREPARED
                    && branch.state != decided
                    && branch.state != BranchState.STORAGE_COMPLETED) {
                throw new XaException("XA branch cannot run phase two from state " + branch.state);
            }
            // 2、registry decision 必须先于 storage terminal；重复同状态 append 是幂等读取。
            if (branch.state == BranchState.PREPARED) {
                if (commit) {
                    registry.decideCommit(xid, branch.transactionId);
                } else {
                    registry.decideRollback(xid, branch.transactionId);
                }
                branch.state = decided;
            }
            // 3、registry COMPLETED 写失败时缓存 storage 已完成，重试只补 registry，不重复触碰 terminal handle。
            if (branch.state != BranchState.STORAGE_COMPLETED) {
                if (commit) {
                    branch.gateway.commitPreparedXa(
                            branch.handle, deadline.remaining("storage XA prepared commit"));
                } else {
                    branch.gateway.rollbackPreparedXa(
                            branch.handle, deadline.remaining("storage XA prepared rollback"));
                }
                branch.committed = commit;
                branch.state = BranchState.STORAGE_COMPLETED;
            } else if (branch.committed != commit) {
                throw new XaException("XA storage already completed the opposite decision: " + xid);
            }
            // 4、此 fsync 成功后 branch 才不再需要运行期 handle。
            registry.completed(xid, branch.transactionId);
            branchesLock.lock();
            try {
                preparedBranches.remove(xid, branch);
            } finally {
                branchesLock.unlock();
            }
        } finally {
            branch.operationLock.unlock();
        }
    }

    /**
     * 在调用方绝对期限内取得单 XID operation owner。等待期间不持 registry/map/storage 锁；超时或中断
     * 不修改 branch 状态，也不把完整 timeout 重新交给后续 phase。
     *
     * @param branch 待串行化的 live branch；必须仍在 preparedBranches 中
     * @param deadline 当前 XA 命令唯一的 monotonic 绝对期限
     * @param stage 用于异常诊断的等待阶段
     * @throws XaException 未在期限内取得 owner 或线程被中断时抛出，branch 状态保持不变
     */
    private static void lockBranch(
            PreparedBranch branch, WaitDeadline deadline, String stage) {
        try {
            if (!branch.operationLock.tryLock(
                    deadline.remaining(stage).toNanos(), TimeUnit.NANOSECONDS)) {
                throw new XaException("XA operation timed out waiting for branch owner: " + branch.xid);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new XaException("XA operation interrupted waiting for branch owner: " + branch.xid,
                    interrupted);
        }
    }

    /**
     * 单条 XA 命令的 monotonic 绝对预算；branch owner 等待与 storage durability 只能消费剩余值。
     *
     * @param startedNanos 命令开始时的单调时钟读数
     * @param budgetNanos 正等待预算；Duration 纳秒转换溢出时取 {@link Long#MAX_VALUE}
     */
    private record WaitDeadline(long startedNanos, long budgetNanos) {

        /**
         * 从已校验的正 Duration 建立一次 XA 命令预算。
         *
         * @param timeout branch owner 与后续 storage durability 共享的正上限
         * @return 绑定当前 monotonic 起点的不可变 deadline
         */
        private static WaitDeadline after(Duration timeout) {
            long nanos;
            try {
                nanos = timeout.toNanos();
            } catch (ArithmeticException overflow) {
                nanos = Long.MAX_VALUE;
            }
            return new WaitDeadline(System.nanoTime(), nanos);
        }

        /**
         * 计算当前阶段可使用的严格正剩余值；耗尽时不再进入新的锁等待或 storage 调用。
         *
         * @param stage 当前 branch owner 或 storage durability 阶段，仅用于诊断
         * @return 不超过原始 XA 命令预算的剩余时长
         * @throws XaException deadline 已耗尽时抛出；调用方保留已有 durable 状态供同方向重试
         */
        private Duration remaining(String stage) {
            long elapsed = System.nanoTime() - startedNanos;
            long remaining = budgetNanos == Long.MAX_VALUE ? Long.MAX_VALUE : budgetNanos - elapsed;
            if (remaining <= 0) {
                throw new XaException("XA operation deadline exhausted at " + stage);
            }
            return Duration.ofNanos(remaining);
        }
    }

    private static BranchState opposite(BranchState state) {
        return state == BranchState.COMMIT_DECIDED
                ? BranchState.ROLLBACK_DECIDED : BranchState.COMMIT_DECIDED;
    }

    private static void validate(XaId xid, long transactionId, SqlStorageGateway gateway,
                                 SqlTransactionHandle handle, Duration timeout) {
        if (xid == null || transactionId <= 0 || gateway == null || handle == null
                || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("XA prepare inputs are invalid");
        }
    }

    /** 每 branch 的运行期状态；不进入持久格式。 */
    private enum BranchState {
        PREPARING,
        PREPARED,
        COMMIT_DECIDED,
        ROLLBACK_DECIDED,
        STORAGE_COMPLETED
    }

    /** opaque prepared branch owner；各字段除 state/committed 外构造后稳定。 */
    private static final class PreparedBranch {
        /** 分支唯一 XID。 */
        private final XaId xid;
        /** registry 与 storage 共用的正 transaction id。 */
        private final TransactionId transactionId;
        /** 创建 handle 的 adapter。 */
        private final SqlStorageGateway gateway;
        /** phase-one/phase-two 使用的不透明能力。 */
        private final SqlTransactionHandle handle;
        /** 只串行当前 XID 的阻塞 IO，不影响其它 branch。 */
        private final ReentrantLock operationLock = new ReentrantLock(true);
        /** 当前进程状态，由 operationLock 保护。 */
        private BranchState state = BranchState.PREPARING;
        /** STORAGE_COMPLETED 时记录已执行方向，由 operationLock 保护。 */
        private boolean committed;

        private PreparedBranch(XaId xid, TransactionId transactionId,
                               SqlStorageGateway gateway, SqlTransactionHandle handle) {
            this.xid = xid;
            this.transactionId = transactionId;
            this.gateway = gateway;
            this.handle = handle;
        }
    }
}
