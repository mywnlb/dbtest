package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
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

    /**
     * 验证 {@code beginCommitAssignsNoAndRemovesFromActive} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
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

    /**
     * 验证 {@code prepareCommitAssignsNoWithoutRemovingActiveTransaction} 所描述的边界场景保持既有领域不变量，不产生方法名明确禁止的副作用。
     */
    @Test
    void prepareCommitAssignsNoWithoutRemovingActiveTransaction() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        TransactionId id = mgr.assignWriteId(t);

        mgr.prepareCommit(t);

        assertEquals(TransactionState.ACTIVE, t.state(), "prepareCommit 只预留提交序号，不进入 COMMITTING");
        assertFalse(t.transactionNo().isNone(), "读写事务在持久化 undo commit 前需要先预留 TransactionNo");
        assertTrue(mgr.system().snapshotActiveReadWriteIds().contains(id.value()),
                "undo onCommit 失败时仍必须能按 active 事务恢复/回滚");

        mgr.commit(t);
        assertEquals(TransactionState.COMMITTED, t.state());
        assertFalse(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));
    }

    /** phase one 释放 ReadView但保留 active 身份；phase two commit 才分配提交号并移除。 */
    @Test
    void preparedCommitRetainsActiveMembershipUntilDecision() {
        TransactionManager manager = newManager();
        Transaction transaction = manager.begin(TransactionOptions.defaults());
        TransactionId id = manager.assignWriteId(transaction);
        manager.readViewManager().openReadView(transaction);

        manager.finishPrepare(transaction);

        assertEquals(TransactionState.PREPARED, transaction.state());
        assertNull(transaction.readView());
        assertTrue(manager.system().snapshotActiveReadWriteIds().contains(id.value()));

        manager.prepareCommitPrepared(transaction);
        assertFalse(transaction.transactionNo().isNone());
        assertEquals(TransactionState.PREPARED, transaction.state());

        manager.commitPrepared(transaction);

        assertEquals(TransactionState.COMMITTED, transaction.state());
        assertFalse(manager.system().snapshotActiveReadWriteIds().contains(id.value()));
    }

    /** prepared rollback 在 inverse/finalization 完成前保留独立重试态与 active 身份。 */
    @Test
    void preparedRollbackRetainsActiveMembershipUntilFinished() {
        TransactionManager manager = newManager();
        Transaction transaction = manager.begin(TransactionOptions.defaults());
        TransactionId id = manager.assignWriteId(transaction);

        manager.finishPrepare(transaction);
        manager.beginPreparedRollback(transaction);

        assertEquals(TransactionState.PREPARED_ROLLING_BACK, transaction.state());
        assertTrue(manager.system().snapshotActiveReadWriteIds().contains(id.value()));

        manager.finishPreparedRollback(transaction);

        assertEquals(TransactionState.ROLLED_BACK, transaction.state());
        assertFalse(manager.system().snapshotActiveReadWriteIds().contains(id.value()));
    }

    /** 普通终态 API 不能替 XA coordinator 对 PREPARED 分支作隐式决议。 */
    @Test
    void ordinaryCommitAndRollbackRejectPreparedTransaction() {
        TransactionManager manager = newManager();
        Transaction transaction = manager.begin(TransactionOptions.defaults());
        manager.assignWriteId(transaction);
        manager.finishPrepare(transaction);

        assertThrows(TransactionStateException.class, () -> manager.commit(transaction));
        assertThrows(TransactionStateException.class, () -> manager.rollback(transaction));
        assertEquals(TransactionState.PREPARED, transaction.state());
    }

    /**
     * 验证 {@code readOnlyCommitAssignsNoTransactionNo} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void readOnlyCommitAssignsNoTransactionNo() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(new TransactionOptions(IsolationLevel.REPEATABLE_READ, true, true));
        assertThrows(TransactionStateException.class, () -> mgr.assignWriteId(t));
        mgr.commit(t);
        assertEquals(TransactionState.COMMITTED, t.state());
        assertTrue(t.transactionNo().isNone(), "read-only txn gets no commit no");
    }

    /**
     * 验证 {@code rollbackRemovesFromActive} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void rollbackRemovesFromActive() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        TransactionId id = mgr.assignWriteId(t);
        mgr.rollback(t);
        assertEquals(TransactionState.ROLLED_BACK, t.state());
        assertFalse(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));
    }

    /**
     * 验证 {@code doubleCommitRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void doubleCommitRejected() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.commit(t);
        assertThrows(TransactionStateException.class, () -> mgr.commit(t));
    }

    /**
     * 验证 {@code rollbackAfterCommitRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rollbackAfterCommitRejected() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.commit(t);
        assertThrows(TransactionStateException.class, () -> mgr.rollback(t));
    }

    /**
     * statement rollback 结果不确定后，事务保持 ACTIVE 以便执行完整 rollback，但不得继续写入或提交。
     */
    @Test
    void rollbackOnlyTransactionRejectsFurtherWritesAndCommitButAllowsFullRollback() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        TransactionId id = mgr.assignWriteId(t);

        mgr.markRollbackOnly(t, new DatabaseRuntimeException("statement rollback outcome is uncertain"));

        assertTrue(t.rollbackOnly());
        assertTrue(t.rollbackOnlyReason().contains("statement rollback outcome is uncertain"));
        assertThrows(TransactionStateException.class, () -> mgr.assignWriteId(t));
        assertThrows(TransactionStateException.class, () -> mgr.prepareCommit(t));
        assertThrows(TransactionStateException.class, () -> mgr.commit(t));
        assertEquals(TransactionState.ACTIVE, t.state(), "rollback-only is an ACTIVE sub-state until full rollback");
        assertTrue(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));

        mgr.rollback(t);
        assertEquals(TransactionState.ROLLED_BACK, t.state());
        assertFalse(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));
    }

    // ---- T1.3d：两阶段 rollback（RollbackService 把 undo 走链夹在 ROLLING_BACK 状态内） ----

    /**
     * 验证 {@code twoPhaseRollbackKeepsActiveDuringRollingBack} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
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

    /**
     * 验证 {@code finishRollbackRequiresRollingBack} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void finishRollbackRequiresRollingBack() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.assignWriteId(t);
        // 未 beginRollback（仍 ACTIVE）直接 finishRollback 非法
        assertThrows(TransactionStateException.class, () -> mgr.finishRollback(t));
    }

    /**
     * 验证 {@code beginRollbackRequiresActive} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void beginRollbackRequiresActive() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.commit(t);
        assertThrows(TransactionStateException.class, () -> mgr.beginRollback(t));
    }

    // ---- T1.4：commit/rollback 释放事务级 ReadView ----

    /**
     * 验证 {@code commitReleasesReadView} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void commitReleasesReadView() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.readViewManager().openReadView(t); // RR 绑定事务级 ReadView
        assertNotNull(t.readView());
        mgr.commit(t);
        assertNull(t.readView(), "commit 移出活跃表后释放 ReadView");
    }

    /**
     * 验证 {@code rollbackReleasesReadView} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void rollbackReleasesReadView() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.readViewManager().openReadView(t);
        assertNotNull(t.readView());
        mgr.rollback(t);
        assertNull(t.readView(), "rollback finish 后释放 ReadView");
    }

    /**
     * 验证 {@code neverOpenedReadViewCommitUnaffected} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void neverOpenedReadViewCommitUnaffected() {
        TransactionManager mgr = newManager();
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.assignWriteId(t);
        mgr.commit(t); // 未开 ReadView，release 为 no-op，不抛
        assertNull(t.readView());
        assertEquals(TransactionState.COMMITTED, t.state());
    }

    /**
     * ReadView 可在 TransactionNo 已分配但持久 commit 尚未发布的窗口创建；仅看 lowLimitNo 会误 purge，
     * creator 可见性复核必须继续挡住该 history，直到快照关闭。
     */
    @Test
    void purgeEligibilityChecksCreatorVisibilityAcrossPreparedCommitWindow() {
        TransactionSystem system = new TransactionSystem();
        TransactionManager manager = new TransactionManager(system);
        Transaction writer = manager.begin(TransactionOptions.defaults());
        TransactionId creator = manager.assignWriteId(writer);
        manager.prepareCommit(writer);

        Transaction reader = manager.begin(TransactionOptions.defaults());
        manager.readViewManager().openReadView(reader);
        assertTrue(writer.transactionNo().value() < system.purgeLowWaterNo().value(),
                "allocated commit number alone appears older than the coarse boundary");
        assertFalse(system.isPurgeEligible(writer.transactionNo(), creator),
                "snapshot captured creator as active before persistent commit");

        manager.commit(writer);
        assertFalse(system.isPurgeEligible(writer.transactionNo(), creator),
                "immutable old snapshot still needs the pre-commit version after writer terminal publication");
        manager.rollback(reader);
        assertTrue(system.isPurgeEligible(writer.transactionNo(), creator));
    }

    /**
     * 即使当前没有 ReadView，已分配提交号但仍留在 active table 的 creator 也不能 purge；否则检查后新建的快照
     * 会把 creator 捕获为活跃事务，却已失去构造旧版本所需的 undo。
     */
    @Test
    void purgeEligibilityRejectsPreparedCommitCreatorWhenNoReadViewExists() {
        TransactionSystem system = new TransactionSystem();
        TransactionManager manager = new TransactionManager(system);
        Transaction writer = manager.begin(TransactionOptions.defaults());
        TransactionId creator = manager.assignWriteId(writer);
        manager.prepareCommit(writer);

        assertFalse(system.isPurgeEligible(writer.transactionNo(), creator),
                "prepared creator remains active even when the live ReadView set is empty");

        manager.commit(writer);
        assertTrue(system.isPurgeEligible(writer.transactionNo(), creator));
    }
}
