package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 事务门面（innodb-transaction-mvcc-design §13.1）。begin/commit/rollback 为纯内存生命周期：读写事务惰性分配
 * 写 id（首次写入），commit 给读写事务分配 {@code transactionNo} 并移出活跃表。
 *
 * <p><b>本片无 undo</b>：commit/rollback 只翻转内存事务状态、维护活跃表，**不刷数据页、不撤销任何已写记录**——
 * 被 rollback 的事务插入的行仍物理留在页上、仍带它的 DB_TRX_ID。真正的数据回滚在 T1.3。
 * statement rollback 失败可把 ACTIVE 事务标记为 rollback-only：事务继续留在活跃表供 full rollback，但普通写入和
 * commit 会在任何提交副作用前被拒绝。
 *
 * <p>本片不提供 {@code current()}：不引入 ThreadLocal 绑定/嵌套 begin/线程切换语义，调用方显式持有并传递
 * {@link Transaction}，避免隐藏全局可变状态。
 */
public final class TransactionManager {

    /** 全局协调器（id/no 分配、活跃表）。 */
    private final TransactionSystem system;
    /**
     * 事务级 ReadView 门面（T1.4）。本管理器拥有它（由同一 {@code system} 构造），commit/finishRollback 经它释放
     * 事务级 ReadView。无状态（RR 缓存挂 Transaction），故按本管理器拥有即可，非全局单例；上层一致性读经
     * {@link #readViewManager()} 取同一实例。
     */
    private final ReadViewManager readViewManager;

    public TransactionManager(TransactionSystem system) {
        if (system == null) {
            throw new DatabaseValidationException("transaction system must not be null");
        }
        this.system = system;
        this.readViewManager = new ReadViewManager(system);
    }

    /** 暴露协调器供测试/上层查询活跃事务快照（不暴露内部可变表）。 */
    public TransactionSystem system() {
        return system;
    }

    /** 暴露本管理器拥有的 ReadView 门面，供一致性读（{@code MvccReader}）取/复用同一实例。 */
    public ReadViewManager readViewManager() {
        return readViewManager;
    }

    /** 开启事务，状态 ACTIVE；读写事务此时不分配写 id（惰性，§7.1）。 */
    public Transaction begin(TransactionOptions options) {
        if (options == null) {
            throw new DatabaseValidationException("transaction options must not be null");
        }
        return new Transaction(options, System.currentTimeMillis());
    }

    /**
     * 首次写入分配写 id（幂等：已分配则返回原 id）。只读事务调用直接拒绝。
     * 调用方在 B+Tree 聚簇写入前调用，再把 {@code txn.transactionId()} 传入写入路径。
     */
    public TransactionId assignWriteId(Transaction txn) {
        requireUsableActive(txn);
        if (txn.readOnly()) {
            throw new TransactionStateException("read-only transaction cannot assign a write id");
        }
        if (!txn.transactionId().isNone()) {
            return txn.transactionId();
        }
        TransactionId id = system.allocateWriteId();
        txn.setTransactionId(id);
        return id;
    }

    /**
     * 为提交预留 {@code TransactionNo}，但事务仍保持 ACTIVE 且留在活跃表。DML facade 需要先把
     * {@code UndoLogManager.onCommit} 的 undo header/slot 状态持久化，再真正移出 active table；否则
     * onCommit 失败后崩溃恢复会把已暴露为 COMMITTED 的事务误判为 ACTIVE undo 并回滚。
     *
     * <p>该方法只做提交序号的幂等预分配，不释放 ReadView、不释放事务锁、不进入 COMMITTING。调用方若后续
     * onCommit 失败，仍可按 ACTIVE 事务执行 rollback；若成功，再调用 {@link #commit(Transaction)} 完成状态转换。
     *
     * @param txn 仍处于 ACTIVE 的事务。
     */
    public void prepareCommit(Transaction txn) {
        requireUsableActive(txn);
        if (!txn.transactionId().isNone() && txn.transactionNo().isNone()) {
            txn.setTransactionNo(system.allocateTransactionNo());
        }
    }

    /**
     * 提交：ACTIVE→COMMITTING→COMMITTED。读写事务分配提交序号并移出活跃表；只读事务（id 仍 NONE）
     * 不分配序号、无需移出。不刷数据页、不撤销记录。
     */
    public void commit(Transaction txn) {
        requireUsableActive(txn);
        txn.transitionTo(TransactionState.COMMITTING);
        if (!txn.transactionId().isNone()) {
            if (txn.transactionNo().isNone()) {
                txn.setTransactionNo(system.allocateTransactionNo());
            }
            system.removeActive(txn.transactionId().value());
        }
        // 移出活跃表后、进入终态前释放事务级 ReadView（T1.4；RC/未开 ReadView 时为 no-op）
        readViewManager.release(txn);
        txn.transitionTo(TransactionState.COMMITTED);
    }

    /**
     * 回滚：ACTIVE→ROLLING_BACK→ROLLED_BACK。读写事务移出活跃表。
     *
     * <p>T1.3c 之前无 undo，本方法只翻状态；T1.3d 起拆为 {@link #beginRollback}/{@link #finishRollback} 两阶段，
     * 本方法是「无 undo 链可走」（只读/未写事务）的便捷组合：{@code RollbackService} 对有 {@code UndoContext} 的
     * 事务改为先 {@code beginRollback}、反向走 undo 链、释放 slot，再 {@code finishRollback}，使撤销发生在真正的
     * {@code ROLLING_BACK} 状态内（设计 §7.6）。本组合行为与旧实现完全一致。
     */
    public void rollback(Transaction txn) {
        beginRollback(txn);
        finishRollback(txn);
    }

    /**
     * 进入回滚：ACTIVE→ROLLING_BACK。供 {@code RollbackService} 在反向走 undo 链前调用，使整段撤销处于
     * {@code ROLLING_BACK} 状态。此阶段**不**移出活跃表——事务在撤销完成前仍是活跃读写事务（设计 §7.6 step 1）。
     */
    void beginRollback(Transaction txn) {
        requireActive(txn);
        txn.transitionTo(TransactionState.ROLLING_BACK);
    }

    /**
     * 收尾回滚：ROLLING_BACK→ROLLED_BACK，读写事务移出活跃表。只有 undo 链**完整**走到 prev=NULL 并释放 slot 后
     * 才调用；单条 undo 失败不应到达此处（{@code RollbackService} 让异常传播、事务停在 {@code ROLLING_BACK} 可重试）。
     *
     * @throws TransactionStateException 当前不在 {@code ROLLING_BACK}（未先 {@link #beginRollback}）。
     */
    void finishRollback(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException("transaction must not be null");
        }
        if (txn.state() != TransactionState.ROLLING_BACK) {
            throw new TransactionStateException("finishRollback requires ROLLING_BACK: " + txn.state());
        }
        if (!txn.transactionId().isNone()) {
            system.removeActive(txn.transactionId().value());
        }
        // 移出活跃表后、进入终态前释放事务级 ReadView（T1.4）
        readViewManager.release(txn);
        txn.transitionTo(TransactionState.ROLLED_BACK);
    }

    /**
     * statement rollback 失败后撤销事务提交资格。事务保持 ACTIVE 和 active-table 成员身份，保证调用方仍能执行
     * full rollback；后续写入、prepareCommit、commit 由 {@link #requireUsableActive} 统一拒绝。
     *
     * @param txn   发生不确定 statement rollback 的 ACTIVE 事务。
     * @param cause 触发失败的原始领域异常，用于保留首个诊断原因。
     */
    void markRollbackOnly(Transaction txn, RuntimeException cause) {
        requireActive(txn);
        if (cause == null) {
            throw new DatabaseValidationException("rollback-only cause must not be null");
        }
        String message = cause.getMessage();
        String reason = cause.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message);
        txn.markRollbackOnly(reason);
    }

    /** 校验事务可继续普通工作；rollback-only 虽保持 ACTIVE，但只能进入完整 rollback。 */
    private static void requireUsableActive(Transaction txn) {
        requireActive(txn);
        if (txn.rollbackOnly()) {
            throw new TransactionStateException(
                    "transaction is rollback-only and cannot continue or commit: " + txn.rollbackOnlyReason());
        }
    }

    private static void requireActive(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException("transaction must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("transaction not ACTIVE: " + txn.state());
        }
    }
}
