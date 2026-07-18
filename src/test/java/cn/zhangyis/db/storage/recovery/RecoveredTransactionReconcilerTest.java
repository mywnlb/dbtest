package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaReason;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaRecord;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaState;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** page3 与 checkpoint/redo 事务证据交叉校验测试。 */
class RecoveredTransactionReconcilerTest {

    private static final SpaceId UNDO_SPACE = SpaceId.of(2);

    /** 没有终态冲突的 ACTIVE page3 slot 必须进入 RECOVERED_ACTIVE 集合并推进 id 高水位。 */
    @Test
    void activePage3SlotBecomesRecoveredActive() {
        RecoveredTransactionSnapshot redo = baseline(0, 3, 2);
        RecoveredUndoSlotEvidence active = active(0, 7);

        RecoveredTransactionReconciliation result = new RecoveredTransactionReconciler()
                .reconcile(redo, Lsn.of(100), List.of(active));

        assertEquals(List.of(active), result.activeSlots());
        assertEquals(RecoveredTransactionState.RECOVERED_ACTIVE,
                result.snapshot().entry(TransactionId.of(7)).orElseThrow().state());
        assertEquals(TransactionId.of(8), result.snapshot().nextTransactionId());
    }

    /** ACTIVE page3 与 redo COMMITTED 终态冲突时不能误回滚已提交事务。 */
    @Test
    void activePage3ConflictingWithCommittedRedoIsFatal() {
        RecoveredTransactionTable table = table(0, 3, 2);
        table.accept(range(), committed(7, 3));

        assertThrows(TransactionRecoveryException.class,
                () -> new RecoveredTransactionReconciler()
                        .reconcile(table.snapshot(), Lsn.of(120), List.of(active(0, 7))));
    }

    /** COMMITTED page3 与相同 redo terminal 证据可合并为 history 输入。 */
    @Test
    void committedPage3MatchingRedoIsAccepted() {
        RecoveredTransactionTable table = table(0, 3, 2);
        table.accept(range(), committed(7, 3));
        RecoveredUndoSlotEvidence committed = committedSlot(0, 7, 3);

        RecoveredTransactionReconciliation result = new RecoveredTransactionReconciler()
                .reconcile(table.snapshot(), Lsn.of(120), List.of(committed));

        assertEquals(List.of(committed), result.committedSlots());
        assertEquals(TransactionNo.of(4), result.snapshot().nextTransactionNo());
    }

    /** terminal delta 已被 checkpoint 过滤时，baseline 覆盖 creator/commitNo 才能信任 COMMITTED page3。 */
    @Test
    void committedPage3WithoutRedoRequiresCoveringBaseline() {
        RecoveredUndoSlotEvidence committed = committedSlot(0, 7, 3);

        RecoveredTransactionReconciliation accepted = new RecoveredTransactionReconciler()
                .reconcile(baseline(10, 8, 4), Lsn.of(120), List.of(committed));
        assertEquals(List.of(committed), accepted.committedSlots());

        assertThrows(TransactionRecoveryException.class,
                () -> new RecoveredTransactionReconciler()
                        .reconcile(baseline(10, 7, 4), Lsn.of(120), List.of(committed)));
        assertThrows(TransactionRecoveryException.class,
                () -> new RecoveredTransactionReconciler()
                        .reconcile(baseline(10, 8, 3), Lsn.of(120), List.of(committed)));
    }

    /** COMMITTED page3 的 commitNo 与 redo terminal 不一致时必须 fail closed。 */
    @Test
    void committedPage3CommitNumberMismatchIsFatal() {
        RecoveredTransactionTable table = table(0, 3, 2);
        table.accept(range(), committed(7, 3));

        assertThrows(TransactionRecoveryException.class,
                () -> new RecoveredTransactionReconciler().reconcile(
                        table.snapshot(), Lsn.of(120), List.of(committedSlot(0, 7, 4))));
    }

    /**
     * 验证 {@code activeTransactionMayOwnOneInsertAndOneUpdateSlot} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void activeTransactionMayOwnOneInsertAndOneUpdateSlot() {
        RecoveredUndoSlotEvidence insert = active(0, 7);
        RecoveredUndoSlotEvidence update = RecoveredUndoSlotEvidence.active(
                UndoSlotId.of(1), PageId.of(UNDO_SPACE, PageNo.of(11)),
                UndoLogKind.UPDATE, TransactionId.of(7));
        RecoveredTransactionReconciliation result = new RecoveredTransactionReconciler()
                .reconcile(baseline(0, 3, 2), Lsn.of(100), List.of(insert, update));
        assertEquals(List.of(insert, update), result.activeSlots());
    }

    /**
     * 验证 {@code duplicateRecoveredKindForOneCreatorIsFatal} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void duplicateRecoveredKindForOneCreatorIsFatal() {
        assertThrows(TransactionRecoveryException.class, () -> new RecoveredTransactionReconciler()
                .reconcile(baseline(0, 3, 2), Lsn.of(100), List.of(active(0, 7), active(1, 7))));
    }

    /**
     * 验证 {@code activeAndCommittedUndoForOneCreatorIsFatal} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void activeAndCommittedUndoForOneCreatorIsFatal() {
        RecoveredTransactionTable table = table(0, 8, 4);
        assertThrows(TransactionRecoveryException.class, () -> new RecoveredTransactionReconciler()
                .reconcile(table.snapshot(), Lsn.of(100), List.of(active(0, 7), committedSlot(1, 7, 3))));
    }

    /**
     * 验证 {@code committedInsertUndoEvidenceIsFatal} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void committedInsertUndoEvidenceIsFatal() {
        assertThrows(TransactionRecoveryException.class, () -> RecoveredUndoSlotEvidence.committed(
                UndoSlotId.of(0), PageId.of(UNDO_SPACE, PageNo.of(10)), UndoLogKind.INSERT,
                TransactionId.of(7), TransactionNo.of(3)));
    }

    /** PREPARED page3 与相同 redo phase-one 证据闭合后进入独立集合，不被当作 recovered-active 回滚。 */
    @Test
    void preparedPage3MatchingRedoIsAccepted() {
        RecoveredTransactionTable table = table(0, 3, 2);
        table.accept(range(), prepared(7));
        RecoveredUndoSlotEvidence prepared = preparedSlot(0, 7, UndoLogKind.INSERT);

        RecoveredTransactionReconciliation result = new RecoveredTransactionReconciler()
                .reconcile(table.snapshot(), Lsn.of(120), List.of(prepared));

        assertEquals(List.of(prepared), result.preparedSlots());
        assertEquals(List.of(), result.activeSlots());
        assertEquals(RecoveredTransactionState.PREPARED,
                result.snapshot().entry(TransactionId.of(7)).orElseThrow().state());
    }

    /** prepare redo 被 checkpoint 回收时，baseline 覆盖 creator 后可由 checksum-protected first page 恢复。 */
    @Test
    void preparedPage3WithoutRedoRequiresCoveringBaseline() {
        RecoveredUndoSlotEvidence prepared = preparedSlot(0, 7, UndoLogKind.UPDATE);

        RecoveredTransactionReconciliation accepted = new RecoveredTransactionReconciler()
                .reconcile(baseline(10, 8, 4), Lsn.of(120), List.of(prepared));
        assertEquals(List.of(prepared), accepted.preparedSlots());

        assertThrows(TransactionRecoveryException.class,
                () -> new RecoveredTransactionReconciler()
                        .reconcile(baseline(10, 7, 4), Lsn.of(120), List.of(prepared)));
    }

    /** 同一 creator 的 INSERT/UPDATE 可以同时 PREPARED，但不能混入 ACTIVE。 */
    @Test
    void preparedTransactionMayOwnTwoLogsButCannotMixStates() {
        RecoveredTransactionTable table = table(0, 3, 2);
        table.accept(range(), prepared(7));
        RecoveredUndoSlotEvidence insert = preparedSlot(0, 7, UndoLogKind.INSERT);
        RecoveredUndoSlotEvidence update = preparedSlot(1, 7, UndoLogKind.UPDATE);

        RecoveredTransactionReconciliation result = new RecoveredTransactionReconciler()
                .reconcile(table.snapshot(), Lsn.of(120), List.of(insert, update));
        assertEquals(List.of(insert, update), result.preparedSlots());

        assertThrows(TransactionRecoveryException.class,
                () -> new RecoveredTransactionReconciler()
                        .reconcile(table.snapshot(), Lsn.of(120), List.of(insert, active(1, 7))));
    }

    private static RecoveredTransactionSnapshot baseline(long lsn, long nextId, long nextNo) {
        return table(lsn, nextId, nextNo).snapshot();
    }

    private static RecoveredTransactionTable table(long lsn, long nextId, long nextNo) {
        return RecoveredTransactionTable.open(Lsn.of(lsn), Optional.of(
                new TransactionRecoveryCheckpoint(
                        Lsn.of(lsn), TransactionId.of(nextId), TransactionNo.of(nextNo))));
    }

    private static RecoveredUndoSlotEvidence active(int slot, long transactionId) {
        return RecoveredUndoSlotEvidence.active(UndoSlotId.of(slot),
                PageId.of(UNDO_SPACE, PageNo.of(10 + slot)), UndoLogKind.INSERT,
                TransactionId.of(transactionId));
    }

    private static RecoveredUndoSlotEvidence committedSlot(int slot, long transactionId, long transactionNo) {
        return RecoveredUndoSlotEvidence.committed(UndoSlotId.of(slot),
                PageId.of(UNDO_SPACE, PageNo.of(10 + slot)), UndoLogKind.UPDATE,
                TransactionId.of(transactionId),
                TransactionNo.of(transactionNo));
    }

    private static RecoveredUndoSlotEvidence preparedSlot(
            int slot, long transactionId, UndoLogKind kind) {
        return RecoveredUndoSlotEvidence.prepared(
                UndoSlotId.of(slot), PageId.of(UNDO_SPACE, PageNo.of(10 + slot)),
                kind, TransactionId.of(transactionId));
    }

    private static TransactionStateDeltaRecord committed(long transactionId, long transactionNo) {
        return new TransactionStateDeltaRecord(
                TransactionId.of(transactionId), TransactionStateDeltaState.ACTIVE,
                TransactionStateDeltaState.COMMITTED, TransactionNo.of(transactionNo),
                TransactionStateDeltaReason.COMMIT);
    }

    private static TransactionStateDeltaRecord prepared(long transactionId) {
        return new TransactionStateDeltaRecord(
                TransactionId.of(transactionId), TransactionStateDeltaState.ACTIVE,
                TransactionStateDeltaState.PREPARED, TransactionNo.NONE,
                TransactionStateDeltaReason.PREPARE);
    }

    private static LogRange range() {
        return new LogRange(Lsn.of(100), Lsn.of(120));
    }
}
