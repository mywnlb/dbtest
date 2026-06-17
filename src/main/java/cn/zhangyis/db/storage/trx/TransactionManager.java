package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 事务门面（innodb-transaction-mvcc-design §13.1）。begin/commit/rollback 为纯内存生命周期：读写事务惰性分配
 * 写 id（首次写入），commit 给读写事务分配 {@code transactionNo} 并移出活跃表。
 *
 * <p><b>本片无 undo</b>：commit/rollback 只翻转内存事务状态、维护活跃表，**不刷数据页、不撤销任何已写记录**——
 * 被 rollback 的事务插入的行仍物理留在页上、仍带它的 DB_TRX_ID。真正的数据回滚在 T1.3。
 *
 * <p>本片不提供 {@code current()}：不引入 ThreadLocal 绑定/嵌套 begin/线程切换语义，调用方显式持有并传递
 * {@link Transaction}，避免隐藏全局可变状态。
 */
public final class TransactionManager {

    /** 全局协调器（id/no 分配、活跃表）。 */
    private final TransactionSystem system;

    public TransactionManager(TransactionSystem system) {
        if (system == null) {
            throw new DatabaseValidationException("transaction system must not be null");
        }
        this.system = system;
    }

    /** 暴露协调器供测试/上层查询活跃事务快照（不暴露内部可变表）。 */
    public TransactionSystem system() {
        return system;
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
        requireActive(txn);
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
     * 提交：ACTIVE→COMMITTING→COMMITTED。读写事务分配提交序号并移出活跃表；只读事务（id 仍 NONE）
     * 不分配序号、无需移出。不刷数据页、不撤销记录。
     */
    public void commit(Transaction txn) {
        requireActive(txn);
        txn.transitionTo(TransactionState.COMMITTING);
        if (!txn.transactionId().isNone()) {
            txn.setTransactionNo(system.allocateTransactionNo());
            system.removeActive(txn.transactionId().value());
        }
        txn.transitionTo(TransactionState.COMMITTED);
    }

    /**
     * 回滚：ACTIVE→ROLLING_BACK→ROLLED_BACK。读写事务移出活跃表。**本片不撤销已写记录**（无 undo）。
     */
    public void rollback(Transaction txn) {
        requireActive(txn);
        txn.transitionTo(TransactionState.ROLLING_BACK);
        if (!txn.transactionId().isNone()) {
            system.removeActive(txn.transactionId().value());
        }
        txn.transitionTo(TransactionState.ROLLED_BACK);
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
