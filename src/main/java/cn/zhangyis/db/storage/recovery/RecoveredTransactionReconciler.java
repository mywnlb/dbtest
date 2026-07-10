package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * checkpoint/redo snapshot 与 page3 undo slot 的纯内存交叉校验器。
 *
 * <p>COMMITTED 无 post-checkpoint redo 时，仅 baseline 已覆盖 creator id 与 commit no 才接受；ACTIVE 只要与
 * terminal/PREPARED 证据冲突就拒绝。该对象无共享状态，所有 page latch 已由调用方在进入本方法前释放。
 */
public final class RecoveredTransactionReconciler {

    /** 合并 page3 证据并返回 rollback/history 分类；任何歧义均抛致命恢复异常。 */
    public RecoveredTransactionReconciliation reconcile(
            RecoveredTransactionSnapshot redoSnapshot,
            Lsn recoveredToLsn,
            List<RecoveredUndoSlotEvidence> page3Slots) {
        if (redoSnapshot == null || recoveredToLsn == null || page3Slots == null) {
            throw new DatabaseValidationException("transaction reconcile inputs must not be null");
        }
        Map<TransactionId, RecoveredTransactionEntry> merged =
                new LinkedHashMap<>(redoSnapshot.entries());
        List<RecoveredUndoSlotEvidence> active = new ArrayList<>();
        List<RecoveredUndoSlotEvidence> committed = new ArrayList<>();
        Set<TransactionId> page3Creators = new HashSet<>();
        long nextId = redoSnapshot.nextTransactionId().value();
        long nextNo = redoSnapshot.nextTransactionNo().value();

        for (RecoveredUndoSlotEvidence slot : page3Slots) {
            if (slot == null) {
                throw new DatabaseValidationException("recovered page3 slot evidence must not be null");
            }
            if (!page3Creators.add(slot.creatorTransactionId())) {
                throw new TransactionRecoveryException(
                        "duplicate page3 creator transaction id: " + slot.creatorTransactionId().value());
            }
            nextId = Math.max(nextId, increment(slot.creatorTransactionId().value(), "transaction id"));
            Optional<RecoveredTransactionEntry> redoEntry =
                    Optional.ofNullable(merged.get(slot.creatorTransactionId()));
            if (slot.state() == RecoveredUndoState.ACTIVE) {
                reconcileActive(slot, redoEntry, recoveredToLsn, merged);
                active.add(slot);
            } else {
                reconcileCommitted(slot, redoEntry, redoSnapshot, recoveredToLsn, merged);
                nextNo = Math.max(nextNo, increment(slot.transactionNo().value(), "transaction no"));
                committed.add(slot);
            }
        }

        RecoveredTransactionSnapshot snapshot = new RecoveredTransactionSnapshot(
                redoSnapshot.baselineCheckpointLsn(),
                redoSnapshot.baselineNextTransactionId(), redoSnapshot.baselineNextTransactionNo(),
                TransactionId.of(nextId), TransactionNo.of(nextNo), merged);
        return new RecoveredTransactionReconciliation(snapshot, active, committed);
    }

    private static void reconcileActive(
            RecoveredUndoSlotEvidence slot,
            Optional<RecoveredTransactionEntry> redoEntry,
            Lsn recoveredToLsn,
            Map<TransactionId, RecoveredTransactionEntry> merged) {
        if (redoEntry.isPresent() && isTerminal(redoEntry.get().state())) {
            throw new TransactionRecoveryException(
                    "ACTIVE page3 slot conflicts with terminal redo evidence: transaction="
                            + slot.creatorTransactionId().value() + ", redoState=" + redoEntry.get().state());
        }
        merged.put(slot.creatorTransactionId(), page3Entry(
                slot, RecoveredTransactionState.RECOVERED_ACTIVE, recoveredToLsn));
    }

    private static void reconcileCommitted(
            RecoveredUndoSlotEvidence slot,
            Optional<RecoveredTransactionEntry> redoEntry,
            RecoveredTransactionSnapshot snapshot,
            Lsn recoveredToLsn,
            Map<TransactionId, RecoveredTransactionEntry> merged) {
        if (redoEntry.isPresent()) {
            RecoveredTransactionEntry evidence = redoEntry.get();
            if (evidence.state() != RecoveredTransactionState.COMMITTED
                    || !evidence.transactionNo().equals(slot.transactionNo())) {
                throw new TransactionRecoveryException(
                        "COMMITTED page3 slot conflicts with redo evidence: transaction="
                                + slot.creatorTransactionId().value() + ", page3No="
                                + slot.transactionNo().value() + ", redo=" + evidence);
            }
            return;
        }
        boolean idCovered = slot.creatorTransactionId().value()
                < snapshot.baselineNextTransactionId().value();
        boolean noCovered = slot.transactionNo().value()
                < snapshot.baselineNextTransactionNo().value();
        if (!idCovered || !noCovered) {
            throw new TransactionRecoveryException(
                    "COMMITTED page3 slot has no redo terminal and is not covered by checkpoint baseline: "
                            + "transaction=" + slot.creatorTransactionId().value() + ", commitNo="
                            + slot.transactionNo().value() + ", baselineNextId="
                            + snapshot.baselineNextTransactionId().value() + ", baselineNextNo="
                            + snapshot.baselineNextTransactionNo().value());
        }
        merged.put(slot.creatorTransactionId(), page3Entry(
                slot, RecoveredTransactionState.COMMITTED, recoveredToLsn));
    }

    private static RecoveredTransactionEntry page3Entry(
            RecoveredUndoSlotEvidence slot, RecoveredTransactionState state, Lsn recoveredToLsn) {
        return new RecoveredTransactionEntry(slot.creatorTransactionId(), state, slot.transactionNo(),
                Optional.empty(), RecoveredTransactionEvidenceSource.PAGE3, recoveredToLsn);
    }

    private static boolean isTerminal(RecoveredTransactionState state) {
        return state == RecoveredTransactionState.COMMITTED
                || state == RecoveredTransactionState.ROLLED_BACK
                || state == RecoveredTransactionState.PREPARED;
    }

    private static long increment(long value, String field) {
        try {
            return Math.addExact(value, 1L);
        } catch (ArithmeticException e) {
            throw new TransactionRecoveryException(
                    "page3 transaction recovery " + field + " overflow: " + value, e);
        }
    }
}
