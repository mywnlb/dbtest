package cn.zhangyis.db.storage.api.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.recovery.RecoveryState;
import cn.zhangyis.db.storage.recovery.RecoveryTrafficGate;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.trx.RollbackService;
import cn.zhangyis.db.storage.trx.RollbackSummary;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.TransactionState;
import cn.zhangyis.db.storage.trx.TransactionStateException;
import cn.zhangyis.db.storage.trx.UndoLogManager;
import cn.zhangyis.db.storage.trx.lock.LockManager;

import java.time.Duration;

/**
 * 存储引擎的 prepared transaction resource-manager facade。
 *
 * <p>本服务不解析 XID，也不保存全局事务目录；外部协调器必须把 XID→TransactionId 决议事实持久化。本服务只保证
 * phase-one/phase-two 的 undo、redo、运行态与锁释放顺序：物理 phase 成功后发布状态，terminal redo fsync 成功后
 * 才释放锁。durability 失败不会选择相反决议，并允许以同一终态命令重试确认。</p>
 */
public final class PreparedTransactionService {

    /** 事务纯内存状态与 active-table owner。 */
    private final TransactionManager transactionManager;
    /** undo first-page、page3 owner、history 与事务 delta 的物理终结入口。 */
    private final UndoLogManager undoLogManager;
    /** prepared rollback 的逐记录 inverse 与 owner drop 入口。 */
    private final RollbackService rollbackService;
    /** redo 强制持久化和有界等待端口。 */
    private final RedoLogManager redo;
    /** 只有普通流量已经开放时才允许 live XA 命令进入。 */
    private final RecoveryTrafficGate recoveryGate;
    /** terminal durable 后统一释放事务锁。 */
    private final LockManager lockManager;

    /**
     * 构造共享生产组合根协作者的 prepared transaction facade。
     *
     * @param transactionManager 事务状态与 active table 管理器
     * @param undoLogManager undo prepare/commit 物理终结器
     * @param rollbackService prepared rollback 执行器
     * @param redo 与 MTR 共用的 durable redo manager
     * @param recoveryGate 启动恢复流量门控
     * @param lockManager 与 DML/current-read 共用的事务锁管理器
     * @throws DatabaseValidationException 任一协作者缺失时抛出
     */
    public PreparedTransactionService(TransactionManager transactionManager,
                                      UndoLogManager undoLogManager,
                                      RollbackService rollbackService,
                                      RedoLogManager redo,
                                      RecoveryTrafficGate recoveryGate,
                                      LockManager lockManager) {
        if (transactionManager == null || undoLogManager == null || rollbackService == null
                || redo == null || recoveryGate == null || lockManager == null) {
            throw new DatabaseValidationException(
                    "prepared transaction service collaborators must not be null");
        }
        this.transactionManager = transactionManager;
        this.undoLogManager = undoLogManager;
        this.rollbackService = rollbackService;
        this.redo = redo;
        this.recoveryGate = recoveryGate;
        this.lockManager = lockManager;
    }

    /**
     * 持久化 phase one 并发布 PREPARED。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>确认 recovery gate 已开放，拒绝恢复期与只读诊断期 live 命令。</li>
     *     <li>单 MTR 把全部普通 undo owner 标为 PREPARED，并追加唯一 PREPARE delta。</li>
     *     <li>释放 ReadView、发布 PREPARED，但保留 active membership 与全部事务锁。</li>
     *     <li>强制 redo fsync 到 phase-one LSN；超时/IO 失败保留 PREPARED 和锁，供协调器查询或重试。</li>
     * </ol>
     *
     * @param command ACTIVE 写事务与正 durability timeout
     * @return phase-one durable 事务 id/LSN
     * @throws PreparedTransactionOperationException gate 未开放、物理 prepare 或 durability 失败时抛出
     */
    public PreparedTransactionPrepareResult prepare(PrepareTransactionCommand command) {
        // 1、live XA 不能与 crash recovery 并行触碰 page3/undo owner。
        requireOpen();
        if (command == null) {
            throw new DatabaseValidationException("prepare command must not be null");
        }
        Transaction transaction = command.transaction();
        try {
            // 2、先持久化物理证据；失败时事务仍 ACTIVE，不发布半个 PREPARED。
            Lsn preparedTo = undoLogManager.onPrepare(transaction);
            // 3、物理 MTR 已提交后才发布运行态；锁与 active owner 刻意保留。
            transactionManager.finishPrepare(transaction);
            // 4、XA prepare 返回必须强持久，不允许继承普通 commit 的弱 durability policy。
            forceDurable(preparedTo, command.durabilityTimeout(), "prepare");
            return new PreparedTransactionPrepareResult(transaction.transactionId(), preparedTo);
        } catch (RuntimeException error) {
            throw wrap("prepared transaction phase one failed for " + transaction.transactionId(), error);
        }
    }

    /**
     * 执行并持久确认 prepared commit 决议。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>PREPARED 首次调用预留提交号；COMMITTED 只作为上次 durability 失败后的同决议确认重试。</li>
     *     <li>首次调用原子 drop INSERT owner、挂 UPDATE history并写 PREPARED_COMMIT delta，再发布 COMMITTED。</li>
     *     <li>强制 terminal redo fsync；失败时保持终态和锁，禁止改走 rollback。</li>
     *     <li>durable 后释放全部事务锁并返回提交号。</li>
     * </ol>
     *
     * @param command PREPARED/可确认重试的 COMMITTED 事务与正 timeout
     * @return durable COMMITTED 结果
     * @throws PreparedTransactionOperationException 物理终结、durability 或锁清理失败时抛出
     */
    public PreparedTransactionCompletionResult commitPrepared(
            CommitPreparedTransactionCommand command) {
        // 1、只允许首次 prepared commit 或已经选择 commit 后的 durability 确认。
        requireOpen();
        if (command == null) {
            throw new DatabaseValidationException("commit prepared command must not be null");
        }
        Transaction transaction = command.transaction();
        TransactionId transactionId = transaction.transactionId();
        try {
            if (transaction.state() == TransactionState.PREPARED) {
                // 2、物理 history/owner 与 terminal redo 成功后才发布 COMMITTED。
                transactionManager.prepareCommitPrepared(transaction);
                undoLogManager.onCommitPrepared(transaction);
                transactionManager.commitPrepared(transaction);
            } else if (transaction.state() != TransactionState.COMMITTED) {
                throw new TransactionStateException(
                        "commit prepared requires PREPARED or COMMITTED retry: " + transaction.state());
            }
            Lsn terminalLsn = redo.currentLsn();
            // 3、终态不等于外部确认；fsync 未完成前仍保留锁。
            forceDurable(terminalLsn, command.durabilityTimeout(), "commit prepared");
            // 4、只有 durable 决议才允许其它事务取得这些资源。
            int released = lockManager.releaseAll(transactionId);
            return new PreparedTransactionCompletionResult(transactionId, transaction.transactionNo(),
                    TransactionState.COMMITTED, terminalLsn, true, released, 0);
        } catch (RuntimeException error) {
            throw wrap("prepared commit failed for " + transactionId, error);
        }
    }

    /**
     * 执行并持久确认 prepared rollback 决议。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>接受 PREPARED、PREPARED_ROLLING_BACK 重试或 ROLLED_BACK durability 确认，拒绝 commit 分支。</li>
     *     <li>逐条反向应用 undo并持久推进 logical heads，随后原子 drop全部 owner并写 PREPARED_ROLLBACK delta。</li>
     *     <li>强制 terminal redo fsync；失败时即使已是 ROLLED_BACK 也继续保留锁。</li>
     *     <li>durable 后释放全部事务锁并返回本次 inverse 数量。</li>
     * </ol>
     *
     * @param command prepared 写事务、稳定聚簇索引和正 timeout
     * @return durable ROLLED_BACK 结果
     * @throws PreparedTransactionOperationException inverse、owner 终结、durability 或锁清理失败时抛出
     */
    public PreparedTransactionCompletionResult rollbackPrepared(
            RollbackPreparedTransactionCommand command) {
        // 1、恢复重试态由 RollbackService 继续消费，终态只重试 durability/lock cleanup。
        requireOpen();
        if (command == null) {
            throw new DatabaseValidationException("rollback prepared command must not be null");
        }
        return rollbackPreparedInternal(
                command.transaction(), command.clusteredIndex(), false,
                command.durabilityTimeout());
    }

    /**
     * 使用 DD exact-version resolver 执行 prepared rollback；public 命令不暴露聚簇索引或页 identity。
     *
     * @param command prepared 写事务与正 durability timeout
     * @return durable ROLLED_BACK 结果
     * @throws PreparedTransactionOperationException resolver 缺失、inverse、物理终结或 durability 失败时抛出
     */
    public PreparedTransactionCompletionResult rollbackPrepared(
            ResolvedRollbackPreparedTransactionCommand command) {
        requireOpen();
        if (command == null) {
            throw new DatabaseValidationException(
                    "resolved rollback prepared command must not be null");
        }
        return rollbackPreparedInternal(
                command.transaction(), null, true, command.durabilityTimeout());
    }

    /**
     * legacy 显式索引与 DD resolver 两种稳定命令共用 phase-two/durability/lock 收尾。
     *
     * @param transaction 待 rollback 或终态确认重试的事务
     * @param clusteredIndex legacy 模式聚簇索引；resolved 模式为 null
     * @param resolved 是否要求 RollbackService 按 undo identity 解析 exact target
     * @param timeout terminal redo durability 等待上限
     * @return durable ROLLED_BACK 结果
     */
    private PreparedTransactionCompletionResult rollbackPreparedInternal(
            Transaction transaction,
            cn.zhangyis.db.storage.btree.BTreeIndex clusteredIndex,
            boolean resolved,
            Duration timeout) {
        TransactionId transactionId = transaction.transactionId();
        try {
            int applied = 0;
            if (transaction.state() == TransactionState.PREPARED
                    || transaction.state() == TransactionState.PREPARED_ROLLING_BACK) {
                // 2、逐条 inverse 不持长期 undo/page latch；finalizer 成功前不发布终态。
                RollbackSummary summary = resolved
                        ? rollbackService.rollbackPrepared(transaction)
                        : rollbackService.rollbackPrepared(transaction, clusteredIndex);
                applied = summary.undoRecordsApplied();
            } else if (transaction.state() != TransactionState.ROLLED_BACK) {
                throw new TransactionStateException(
                        "rollback prepared requires prepared state or ROLLED_BACK retry: "
                                + transaction.state());
            }
            Lsn terminalLsn = redo.currentLsn();
            // 3、终态 redo 未 durable 时不能对外确认或释放隔离资源。
            forceDurable(terminalLsn, timeout, "rollback prepared");
            // 4、锁清理最后发生；失败可用相同 ROLLED_BACK 命令幂等重试。
            int released = lockManager.releaseAll(transactionId);
            return new PreparedTransactionCompletionResult(transactionId, transaction.transactionNo(),
                    TransactionState.ROLLED_BACK, terminalLsn, true, released, applied);
        } catch (RuntimeException error) {
            throw wrap("prepared rollback failed for " + transactionId, error);
        }
    }

    /** 强制 redo 文件并有界确认目标 LSN；本方法不持事务锁、page latch 或 MTR memo。 */
    private void forceDurable(Lsn target, Duration timeout, String phase) {
        redo.flush();
        if (!redo.waitFlushed(target, timeout)) {
            throw new PreparedTransactionOperationException(
                    phase + " redo did not become durable before timeout: target=" + target.value());
        }
    }

    /** live prepared API 只在 crash recovery 已完整开放用户流量后可用。 */
    private void requireOpen() {
        if (recoveryGate.state() != RecoveryState.OPEN) {
            throw new PreparedTransactionOperationException(
                    "prepared transaction API requires OPEN recovery gate: " + recoveryGate.state());
        }
    }

    /** 保留已有项目领域异常；其余运行时失败统一包装并保留 cause。 */
    private static PreparedTransactionOperationException wrap(String message, RuntimeException error) {
        if (error instanceof PreparedTransactionOperationException preparedError) {
            return preparedError;
        }
        if (error instanceof DatabaseRuntimeException databaseError) {
            return new PreparedTransactionOperationException(message, databaseError);
        }
        return new PreparedTransactionOperationException(message, error);
    }
}
