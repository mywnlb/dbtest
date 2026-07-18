package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.undo.UndoLogKind;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * checkpoint/redo snapshot 与 page3 undo slot 的纯内存交叉校验器。
 *
 * <p>COMMITTED 无 post-checkpoint redo 时，仅 baseline 已覆盖 creator id 与 commit no 才接受；PREPARED
 * 在 baseline 覆盖 creator id 时可由 checksum-protected first-page 恢复。ACTIVE 与任何 terminal/PREPARED
 * 证据冲突都拒绝。该对象无共享状态，所有 page latch 已由调用方在进入本方法前释放。
 */
public final class RecoveredTransactionReconciler {

    /** 合并 page3 证据并返回 rollback/history 分类；任何歧义均抛致命恢复异常。
     *
     * @param redoSnapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param page3Slots 参与 {@code reconcile} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code reconcile} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
        List<RecoveredUndoSlotEvidence> prepared = new ArrayList<>();
        List<RecoveredUndoSlotEvidence> committed = new ArrayList<>();
        validateCreatorGroups(page3Slots);
        long nextId = redoSnapshot.nextTransactionId().value();
        long nextNo = redoSnapshot.nextTransactionNo().value();

        for (RecoveredUndoSlotEvidence slot : page3Slots) {
            if (slot == null) {
                throw new DatabaseValidationException("recovered page3 slot evidence must not be null");
            }
            nextId = Math.max(nextId, increment(slot.creatorTransactionId().value(), "transaction id"));
            Optional<RecoveredTransactionEntry> redoEntry =
                    Optional.ofNullable(merged.get(slot.creatorTransactionId()));
            switch (slot.state()) {
                case ACTIVE -> {
                    reconcileActive(slot, redoEntry, recoveredToLsn, merged);
                    active.add(slot);
                }
                case PREPARED -> {
                    reconcilePrepared(slot, redoEntry, redoSnapshot, recoveredToLsn, merged);
                    prepared.add(slot);
                }
                case COMMITTED -> {
                    reconcileCommitted(slot, redoEntry, redoSnapshot, recoveredToLsn, merged);
                    nextNo = Math.max(nextNo, increment(slot.transactionNo().value(), "transaction no"));
                    committed.add(slot);
                }
            }
        }

        RecoveredTransactionSnapshot snapshot = new RecoveredTransactionSnapshot(
                redoSnapshot.baselineCheckpointLsn(),
                redoSnapshot.baselineNextTransactionId(), redoSnapshot.baselineNextTransactionNo(),
                TransactionId.of(nextId), TransactionNo.of(nextNo), merged);
        return new RecoveredTransactionReconciliation(snapshot, active, prepared, committed);
    }

    /** 一个 creator 最多各有一条 INSERT/UPDATE ACTIVE log；COMMITTED 只能是单独的 UPDATE log。 */
    private static void validateCreatorGroups(List<RecoveredUndoSlotEvidence> slots) {
        Map<TransactionId, EnumMap<UndoLogKind, RecoveredUndoSlotEvidence>> groups = new LinkedHashMap<>();
        for (RecoveredUndoSlotEvidence slot : slots) {
            if (slot == null) {
                throw new DatabaseValidationException("recovered page3 slot evidence must not be null");
            }
            EnumMap<UndoLogKind, RecoveredUndoSlotEvidence> byKind = groups.computeIfAbsent(
                    slot.creatorTransactionId(), ignored -> new EnumMap<>(UndoLogKind.class));
            if (byKind.putIfAbsent(slot.kind(), slot) != null) {
                throw new TransactionRecoveryException("duplicate " + slot.kind()
                        + " undo log for recovered transaction " + slot.creatorTransactionId().value());
            }
        }
        for (Map.Entry<TransactionId, EnumMap<UndoLogKind, RecoveredUndoSlotEvidence>> group : groups.entrySet()) {
            boolean hasActive = group.getValue().values().stream()
                    .anyMatch(slot -> slot.state() == RecoveredUndoState.ACTIVE);
            boolean hasCommitted = group.getValue().values().stream()
                    .anyMatch(slot -> slot.state() == RecoveredUndoState.COMMITTED);
            boolean hasPrepared = group.getValue().values().stream()
                    .anyMatch(slot -> slot.state() == RecoveredUndoState.PREPARED);
            int stateCount = (hasActive ? 1 : 0) + (hasPrepared ? 1 : 0) + (hasCommitted ? 1 : 0);
            if (stateCount > 1) {
                throw new TransactionRecoveryException(
                        "recovered transaction mixes ACTIVE/PREPARED/COMMITTED undo: "
                        + group.getKey().value());
            }
            if (hasCommitted && group.getValue().size() != 1) {
                throw new TransactionRecoveryException("COMMITTED transaction must retain only one UPDATE undo log: "
                        + group.getKey().value());
            }
        }
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

    /**
     * 校验当前状态后推进崩溃恢复状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param slot 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param redoEntry 可选的 {@code redoEntry}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param merged 参与 {@code reconcileCommitted} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private static void reconcileCommitted(
            RecoveredUndoSlotEvidence slot,
            Optional<RecoveredTransactionEntry> redoEntry,
            RecoveredTransactionSnapshot snapshot,
            Lsn recoveredToLsn,
            Map<TransactionId, RecoveredTransactionEntry> merged) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
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
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        boolean idCovered = slot.creatorTransactionId().value()
                < snapshot.baselineNextTransactionId().value();
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
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
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        merged.put(slot.creatorTransactionId(), page3Entry(
                slot, RecoveredTransactionState.COMMITTED, recoveredToLsn));
    }

    /**
     * 合并 PREPARED first-page 证据。post-checkpoint redo 存在时必须精确同态；redo 已被 checkpoint 回收时，
     * baseline 必须覆盖 creator id，避免把 checkpoint 之后凭空出现的损坏 header 当作合法 phase one。
     */
    private static void reconcilePrepared(
            RecoveredUndoSlotEvidence slot,
            Optional<RecoveredTransactionEntry> redoEntry,
            RecoveredTransactionSnapshot snapshot,
            Lsn recoveredToLsn,
            Map<TransactionId, RecoveredTransactionEntry> merged) {
        if (redoEntry.isPresent()) {
            RecoveredTransactionEntry evidence = redoEntry.orElseThrow();
            if (evidence.state() != RecoveredTransactionState.PREPARED
                    || !evidence.transactionNo().isNone()) {
                throw new TransactionRecoveryException(
                        "PREPARED page3 slot conflicts with redo evidence: transaction="
                                + slot.creatorTransactionId().value() + ", redo=" + evidence);
            }
            return;
        }
        if (slot.creatorTransactionId().value() >= snapshot.baselineNextTransactionId().value()) {
            throw new TransactionRecoveryException(
                    "PREPARED page3 slot has no redo phase-one evidence and is not covered by checkpoint baseline: "
                            + "transaction=" + slot.creatorTransactionId().value()
                            + ", baselineNextId=" + snapshot.baselineNextTransactionId().value());
        }
        merged.put(slot.creatorTransactionId(), page3Entry(
                slot, RecoveredTransactionState.PREPARED, recoveredToLsn));
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
