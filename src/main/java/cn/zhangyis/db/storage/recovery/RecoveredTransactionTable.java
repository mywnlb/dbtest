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

    /**
     * 创建 {@code RecoveredTransactionTable}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param baseline 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     */
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
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param redoCheckpointLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param baseline 可选的 {@code baseline}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @return {@code open} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    public static RecoveredTransactionTable open(
            Lsn redoCheckpointLsn, Optional<TransactionRecoveryCheckpoint> baseline) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        if (redoCheckpointLsn == null || baseline == null) {
            throw new DatabaseValidationException("transaction recovery table checkpoint/baseline must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        TransactionRecoveryCheckpoint selected;
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
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
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return new RecoveredTransactionTable(selected);
    }

    /**
     * 顺序合并一条事务状态 delta。handler 已保证 batch 顺序；本方法只维护恢复证据和 counter，不执行事务状态机。
     *
     * @param range redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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

    /** 复制当前表为不可变 snapshot，后续 page3 校验不会接触内部可变映射。
     *
     * @return {@code snapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
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
        if (prior.equals(next)) {
            evidenceByTransaction.put(next.transactionId(), new Evidence(next, endLsn));
            return;
        }
        if (isTerminal(prior.toState())) {
            throw conflict(prior, next);
        }
        if (prior.toState() != next.fromState()) {
            throw conflict(prior, next);
        }
        evidenceByTransaction.put(next.transactionId(), new Evidence(next, endLsn));
    }

    /**
     * 校验 {@code validateRecord} 涉及的崩溃恢复结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private static void validateRecord(TransactionStateDeltaRecord record) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        if (record.transactionId().isNone()) {
            throw new TransactionRecoveryException("transaction state redo has NONE transaction id");
        }
        boolean commit = (record.fromState() == TransactionStateDeltaState.ACTIVE
                || record.fromState() == TransactionStateDeltaState.COMMITTING)
                && record.toState() == TransactionStateDeltaState.COMMITTED
                && record.reason() == TransactionStateDeltaReason.COMMIT
                && !record.transactionNo().isNone();
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        boolean liveRollback = record.fromState() == TransactionStateDeltaState.ROLLING_BACK
                && record.toState() == TransactionStateDeltaState.ROLLED_BACK
                && record.reason() == TransactionStateDeltaReason.ROLLBACK
                && record.transactionNo().isNone();
        boolean recoveryRollback = record.fromState() == TransactionStateDeltaState.ACTIVE
                && record.toState() == TransactionStateDeltaState.ROLLED_BACK
                && record.reason() == TransactionStateDeltaReason.RECOVERY_ROLLBACK
                && record.transactionNo().isNone();
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        boolean prepare = record.fromState() == TransactionStateDeltaState.ACTIVE
                && record.toState() == TransactionStateDeltaState.PREPARED
                && record.reason() == TransactionStateDeltaReason.PREPARE
                && record.transactionNo().isNone();
        boolean preparedCommit = record.fromState() == TransactionStateDeltaState.PREPARED
                && record.toState() == TransactionStateDeltaState.COMMITTED
                && record.reason() == TransactionStateDeltaReason.PREPARED_COMMIT
                && !record.transactionNo().isNone();
        boolean preparedRollback = record.fromState() == TransactionStateDeltaState.PREPARED
                && record.toState() == TransactionStateDeltaState.ROLLED_BACK
                && record.reason() == TransactionStateDeltaReason.PREPARED_ROLLBACK
                && record.transactionNo().isNone();
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        if (!commit && !liveRollback && !recoveryRollback
                && !prepare && !preparedCommit && !preparedRollback) {
            throw new TransactionRecoveryException(
                    "unsupported transaction state delta tuple: " + record);
        }
    }

    private static boolean isTerminal(TransactionStateDeltaState state) {
        return state == TransactionStateDeltaState.COMMITTED
                || state == TransactionStateDeltaState.ROLLED_BACK;
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

    /**
     * 封装崩溃恢复中 {@code Evidence} 的槽位、预留或阶段结果；组件在创建时交叉校验，使恢复和释放路径能区分已完成与剩余工作。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param endLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     */
    private record Evidence(TransactionStateDeltaRecord record, Lsn endLsn) {
    }
}
