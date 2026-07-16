package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.storage.trx.Transaction;

import java.util.concurrent.locks.ReentrantLock;

/** adapter 私有 handle；SQL 包只能看到 marker interface，不能取出真实 Transaction。 */
final class EngineSqlTransactionHandle implements SqlTransactionHandle {
    enum State { ACTIVE, COMMITTED, ROLLED_BACK, FAILED }

    /** 防止同一不透明 handle 被两个调用线程同时用于 statement/commit/rollback。 */
    final ReentrantLock operationLock = new ReentrantLock(true);
    final DefaultSqlStorageGateway owner;
    final Transaction transaction;
    State state = State.ACTIVE;
    boolean wrote;

    EngineSqlTransactionHandle(DefaultSqlStorageGateway owner, Transaction transaction) {
        this.owner = owner;
        this.transaction = transaction;
    }
}
