package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.storage.SqlSavepointHandle;
import cn.zhangyis.db.storage.trx.EmptyUndoBoundary;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionSavepoint;
import cn.zhangyis.db.storage.trx.lock.LockSavepoint;

/**
 * engine adapter 私有保存点能力，同时绑定 transaction identity、undo 边界和锁获取边界。
 * Session 只能持 marker interface，不能据此访问 storage 事务或物理日志。
 */
final class EngineSqlSavepointHandle implements SqlSavepointHandle {

    /** 保存点创建时 undo context 是否已经存在。 */
    enum BoundaryKind {
        /** 事务尚未首写，使用一次性空 undo 能力。 */
        EMPTY_UNDO,
        /** 事务已有 undo context，使用双 logical-head 保存点。 */
        UNDO_SAVEPOINT
    }

    /** 创建能力的 gateway，防止跨 DatabaseEngine 误用。 */
    final DefaultSqlStorageGateway owner;
    /** 保存点所属运行期事务；首写前可能还没有真实 TransactionId。 */
    final Transaction transaction;
    /** undo 边界种类，决定 rollback/release 分派。 */
    final BoundaryKind boundaryKind;
    /** 空边界能力；仅 EMPTY_UNDO 非空。 */
    final EmptyUndoBoundary emptyBoundary;
    /** 双 undo-head 保存点；仅 UNDO_SAVEPOINT 非空。 */
    final TransactionSavepoint undoSavepoint;
    /** 创建保存点时的 LockManager request 高水位。 */
    final LockSavepoint lockSavepoint;
    /** 句柄一次性生命周期；由 transaction handle 串行锁保护。 */
    boolean open = true;

    private EngineSqlSavepointHandle(DefaultSqlStorageGateway owner, Transaction transaction,
                                     BoundaryKind boundaryKind, EmptyUndoBoundary emptyBoundary,
                                     TransactionSavepoint undoSavepoint,
                                     LockSavepoint lockSavepoint) {
        if (owner == null || transaction == null || boundaryKind == null || lockSavepoint == null
                || boundaryKind == BoundaryKind.EMPTY_UNDO
                && (emptyBoundary == null || undoSavepoint != null)
                || boundaryKind == BoundaryKind.UNDO_SAVEPOINT
                && (emptyBoundary != null || undoSavepoint == null)) {
            throw new DatabaseValidationException("invalid engine SQL savepoint handle fields");
        }
        this.owner = owner;
        this.transaction = transaction;
        this.boundaryKind = boundaryKind;
        this.emptyBoundary = emptyBoundary;
        this.undoSavepoint = undoSavepoint;
        this.lockSavepoint = lockSavepoint;
    }

    static EngineSqlSavepointHandle empty(DefaultSqlStorageGateway owner, Transaction transaction,
                                          EmptyUndoBoundary boundary, LockSavepoint locks) {
        return new EngineSqlSavepointHandle(owner, transaction, BoundaryKind.EMPTY_UNDO,
                boundary, null, locks);
    }

    static EngineSqlSavepointHandle undo(DefaultSqlStorageGateway owner, Transaction transaction,
                                         TransactionSavepoint savepoint, LockSavepoint locks) {
        return new EngineSqlSavepointHandle(owner, transaction, BoundaryKind.UNDO_SAVEPOINT,
                null, savepoint, locks);
    }
}
