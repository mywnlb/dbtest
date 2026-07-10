package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaReason;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaRecord;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaSink;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaState;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 恢复线程独占的事务证据表。
 *
 * <p>对象在 redo replay 前以 sidecar 建立 counter 基线，随后按文件顺序接收 delta。它故意非线程安全：
 * recovery gate 关闭期间只有 recovery 线程拥有该对象，不需要伪造全局大锁。终态一旦出现，只接受完全相同
 * record 的幂等重放；任何状态、提交号、原因或转换冲突都按致命恢复错误处理。
 */
public final class RecoveredTransactionTable implements TransactionStateDeltaSink {

    /** 按事务 id 保存最新完整 redo record 与证据 LSN；只由 recovery 线程修改。 */
    private final Map<TransactionId, Evidence> evidenceByTransaction = new LinkedHashMap<>();
    /** 下一次可分配事务 id 的保守高水位。 */
    private long nextTransactionId;
    /** 下一次可分配提交号的保守高水位。 */
    private long nextTransactionNo;
    /** sidecar 原始 id 高水位；redo 合并时不修改。 */
    private final TransactionId baselineNextTransactionId;
    /** sidecar 原始提交号高水位；redo 合并时不修改。 */
    private final TransactionNo baselineNextTransactionNo;
    /** sidecar 声明已由 checkpoint 覆盖的 redo LSN；恢复完整尾不得落后。 */
    private final Lsn baselineCheckpointLsn;

    private RecoveredTransactionTable(TransactionRecoveryCheckpoint baseline) {
        this.baselineCheckpointLsn = baseline.checkpointLsn();
        this.baselineNextTransactionId = baseline.nextTransactionId();
        this.baselineNextTransactionNo = baseline.nextTransactionNo();
        this.nextTransactionId = baseline.nextTransactionId().value();
        this.nextTransactionNo = baseline.nextTransactionNo().value();
    }

    /**
     * 根据 redo checkpoint 与可选 sidecar 创建恢复表。非零 redo checkpoint 必须有覆盖它的权威 sidecar；
     * sidecar 较新表示崩溃发生在 sidecar force 与 redo label force 之间，可以安全接受其保守高水位。
     */
    public static RecoveredTransactionTable open(
            Lsn redoCheckpointLsn, Optional<TransactionRecoveryCheckpoint> baseline) {
        if (redoCheckpointLsn == null || baseline == null) {
            throw new DatabaseValidationException("transaction recovery table checkpoint/baseline must not be null");
        }
        TransactionRecoveryCheckpoint selected;
        if (baseline.isEmpty()) {
            if (redoCheckpointLsn.value() != 0) {
                throw new TransactionRecoveryException(
                        "non-zero redo checkpoint has no valid transaction recovery sidecar: checkpoint="
                                + redoCheckpointLsn.value());
            }
            selected = TransactionRecoveryCheckpoint.initial();
        } else {
            selected = baseline.get();
            if (selected.checkpointLsn().value() < redoCheckpointLsn.value()) {
                throw new TransactionRecoveryException(
                        "transaction recovery sidecar is behind redo checkpoint: sidecar="
                                + selected.checkpointLsn().value() + ", redo=" + redoCheckpointLsn.value());
            }
        }
        return new RecoveredTransactionTable(selected);
    }

    /**
     * 顺序合并一条事务状态 delta。handler 已保证 batch 顺序；本方法只维护恢复证据和 counter，不执行事务状态机。
     */
    @Override
    public void accept(LogRange range, TransactionStateDeltaRecord record) {
        if (range == null || record == null) {
            throw new DatabaseValidationException("transaction recovery delta range/record must not be null");
        }
        validateRecord(record);
        Evidence previous = evidenceByTransaction.get(record.transactionId());
        if (previous != null) {
            mergeWithPrevious(previous, record, range.end());
        } else {
            evidenceByTransaction.put(record.transactionId(), new Evidence(record, range.end()));
        }
        nextTransactionId = Math.max(nextTransactionId, increment(record.transactionId().value(), "transaction id"));
        if (!record.transactionNo().isNone()) {
            nextTransactionNo = Math.max(nextTransactionNo,
                    increment(record.transactionNo().value(), "transaction no"));
        }
    }

    /** 复制当前表为不可变 snapshot，后续 page3 校验不会接触内部可变映射。 */
    public RecoveredTransactionSnapshot snapshot() {
        Map<TransactionId, RecoveredTransactionEntry> entries = new LinkedHashMap<>();
        evidenceByTransaction.forEach((transactionId, evidence) -> entries.put(transactionId,
                new RecoveredTransactionEntry(transactionId,
                        RecoveredTransactionState.fromRedo(evidence.record().toState()),
                        evidence.record().transactionNo(), Optional.of(evidence.record().reason()),
                        RecoveredTransactionEvidenceSource.REDO, evidence.endLsn())));
        return new RecoveredTransactionSnapshot(
                baselineCheckpointLsn, baselineNextTransactionId, baselineNextTransactionNo,
                TransactionId.of(nextTransactionId), TransactionNo.of(nextTransactionNo), entries);
    }

    private void mergeWithPrevious(Evidence previous, TransactionStateDeltaRecord next, Lsn endLsn) {
        TransactionStateDeltaRecord prior = previous.record();
        if (isTerminal(prior.toState())) {
            if (!prior.equals(next)) {
                throw conflict(prior, next);
            }
            evidenceByTransaction.put(next.transactionId(), new Evidence(next, endLsn));
            return;
        }
        if (prior.toState() != next.fromState()) {
            throw conflict(prior, next);
        }
        evidenceByTransaction.put(next.transactionId(), new Evidence(next, endLsn));
    }

    private static void validateRecord(TransactionStateDeltaRecord record) {
        if (record.transactionId().isNone()) {
            throw new TransactionRecoveryException("transaction state redo has NONE transaction id");
        }
        if (record.fromState() == TransactionStateDeltaState.PREPARED
                || record.toState() == TransactionStateDeltaState.PREPARED) {
            throw new TransactionRecoveryException(
                    "PREPARED transaction requires an XA recovery coordinator: transaction="
                            + record.transactionId().value());
        }
        boolean commit = (record.fromState() == TransactionStateDeltaState.ACTIVE
                || record.fromState() == TransactionStateDeltaState.COMMITTING)
                && record.toState() == TransactionStateDeltaState.COMMITTED
                && record.reason() == TransactionStateDeltaReason.COMMIT
                && !record.transactionNo().isNone();
        boolean liveRollback = record.fromState() == TransactionStateDeltaState.ROLLING_BACK
                && record.toState() == TransactionStateDeltaState.ROLLED_BACK
                && record.reason() == TransactionStateDeltaReason.ROLLBACK
                && record.transactionNo().isNone();
        boolean recoveryRollback = record.fromState() == TransactionStateDeltaState.ACTIVE
                && record.toState() == TransactionStateDeltaState.ROLLED_BACK
                && record.reason() == TransactionStateDeltaReason.RECOVERY_ROLLBACK
                && record.transactionNo().isNone();
        if (!commit && !liveRollback && !recoveryRollback) {
            throw new TransactionRecoveryException(
                    "unsupported transaction state delta tuple in recovery v1: " + record);
        }
    }

    private static boolean isTerminal(TransactionStateDeltaState state) {
        return state == TransactionStateDeltaState.COMMITTED
                || state == TransactionStateDeltaState.ROLLED_BACK
                || state == TransactionStateDeltaState.PREPARED;
    }

    private static TransactionRecoveryException conflict(
            TransactionStateDeltaRecord prior, TransactionStateDeltaRecord next) {
        return new TransactionRecoveryException(
                "conflicting transaction recovery delta for transaction " + next.transactionId().value()
                        + ": prior=" + prior + ", next=" + next);
    }

    private static long increment(long value, String field) {
        try {
            return Math.addExact(value, 1L);
        } catch (ArithmeticException e) {
            throw new TransactionRecoveryException("transaction recovery " + field + " overflow: " + value, e);
        }
    }

    private record Evidence(TransactionStateDeltaRecord record, Lsn endLsn) {
    }
}
