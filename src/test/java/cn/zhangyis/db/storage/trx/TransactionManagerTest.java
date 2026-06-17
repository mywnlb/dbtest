package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TransactionManager 生命周期：惰性写 id、commit 分配 no、活跃表维护、只读语义、非法状态。 */
class TransactionManagerTest {

    private TransactionManager newManager() {
        return new TransactionManager(new TransactionSystem());
    }

    @Test
    void beginCommitAssignsNoAndRemovesFromActive() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        assertEquals(TransactionState.ACTIVE, t.state());
        assertTrue(t.transactionId().isNone(), "rw txn id is lazy");

        TransactionId id = mgr.assignWriteId(t);
        assertFalse(id.isNone());
        assertEquals(id, mgr.assignWriteId(t), "assignWriteId idempotent");
        assertTrue(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));

        mgr.commit(t);
        assertEquals(TransactionState.COMMITTED, t.state());
        assertFalse(t.transactionNo().isNone(), "rw commit assigns transactionNo");
        assertFalse(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));
    }

    @Test
    void readOnlyCommitAssignsNoTransactionNo() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(new TransactionOptions(IsolationLevel.REPEATABLE_READ, true, true));
        assertThrows(TransactionStateException.class, () -> mgr.assignWriteId(t));
        mgr.commit(t);
        assertEquals(TransactionState.COMMITTED, t.state());
        assertTrue(t.transactionNo().isNone(), "read-only txn gets no commit no");
    }

    @Test
    void rollbackRemovesFromActive() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        TransactionId id = mgr.assignWriteId(t);
        mgr.rollback(t);
        assertEquals(TransactionState.ROLLED_BACK, t.state());
        assertFalse(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));
    }

    @Test
    void doubleCommitRejected() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.commit(t);
        assertThrows(TransactionStateException.class, () -> mgr.commit(t));
    }

    @Test
    void rollbackAfterCommitRejected() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.commit(t);
        assertThrows(TransactionStateException.class, () -> mgr.rollback(t));
    }
}
