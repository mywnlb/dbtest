package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    // ---- T1.3d：两阶段 rollback（RollbackService 把 undo 走链夹在 ROLLING_BACK 状态内） ----

    @Test
    void twoPhaseRollbackKeepsActiveDuringRollingBack() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        TransactionId id = mgr.assignWriteId(t);

        mgr.beginRollback(t);
        // ROLLING_BACK 期间事务仍在活跃表（undo 走链尚未收尾），符合设计 §7.6
        assertEquals(TransactionState.ROLLING_BACK, t.state());
        assertTrue(mgr.system().snapshotActiveReadWriteIds().contains(id.value()),
                "txn stays active until finishRollback");

        mgr.finishRollback(t);
        assertEquals(TransactionState.ROLLED_BACK, t.state());
        assertFalse(mgr.system().snapshotActiveReadWriteIds().contains(id.value()),
                "finishRollback removes from active table");
    }

    @Test
    void finishRollbackRequiresRollingBack() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.assignWriteId(t);
        // 未 beginRollback（仍 ACTIVE）直接 finishRollback 非法
        assertThrows(TransactionStateException.class, () -> mgr.finishRollback(t));
    }

    @Test
    void beginRollbackRequiresActive() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.commit(t);
        assertThrows(TransactionStateException.class, () -> mgr.beginRollback(t));
    }

    // ---- T1.4：commit/rollback 释放事务级 ReadView ----

    @Test
    void commitReleasesReadView() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.readViewManager().openReadView(t); // RR 绑定事务级 ReadView
        assertNotNull(t.readView());
        mgr.commit(t);
        assertNull(t.readView(), "commit 移出活跃表后释放 ReadView");
    }

    @Test
    void rollbackReleasesReadView() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.readViewManager().openReadView(t);
        assertNotNull(t.readView());
        mgr.rollback(t);
        assertNull(t.readView(), "rollback finish 后释放 ReadView");
    }

    @Test
    void neverOpenedReadViewCommitUnaffected() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.assignWriteId(t);
        mgr.commit(t); // 未开 ReadView，release 为 no-op，不抛
        assertNull(t.readView());
        assertEquals(TransactionState.COMMITTED, t.state());
    }
}
