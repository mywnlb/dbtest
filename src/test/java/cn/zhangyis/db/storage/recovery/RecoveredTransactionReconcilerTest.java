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

    @Test
    void duplicateRecoveredKindForOneCreatorIsFatal() {
        assertThrows(TransactionRecoveryException.class, () -> new RecoveredTransactionReconciler()
                .reconcile(baseline(0, 3, 2), Lsn.of(100), List.of(active(0, 7), active(1, 7))));
    }

    @Test
    void activeAndCommittedUndoForOneCreatorIsFatal() {
        RecoveredTransactionTable table = table(0, 8, 4);
        assertThrows(TransactionRecoveryException.class, () -> new RecoveredTransactionReconciler()
                .reconcile(table.snapshot(), Lsn.of(100), List.of(active(0, 7), committedSlot(1, 7, 3))));
    }

    @Test
    void committedInsertUndoEvidenceIsFatal() {
        assertThrows(TransactionRecoveryException.class, () -> RecoveredUndoSlotEvidence.committed(
                UndoSlotId.of(0), PageId.of(UNDO_SPACE, PageNo.of(10)), UndoLogKind.INSERT,
                TransactionId.of(7), TransactionNo.of(3)));
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

    private static TransactionStateDeltaRecord committed(long transactionId, long transactionNo) {
        return new TransactionStateDeltaRecord(
                TransactionId.of(transactionId), TransactionStateDeltaState.ACTIVE,
                TransactionStateDeltaState.COMMITTED, TransactionNo.of(transactionNo),
                TransactionStateDeltaReason.COMMIT);
    }

    private static LogRange range() {
        return new LogRange(Lsn.of(100), Lsn.of(120));
    }
}
