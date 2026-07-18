package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.SpaceId;

import java.util.List;
import java.util.Set;

/**
 * 一次 crash recovery 的不可变结果快照。它用于启动日志、测试断言和后续 metrics 暴露。
 *
 * @param mode 恢复模式。
 * @param state 恢复结束状态。
 * @param checkpointLsn 本次恢复使用的 checkpoint LSN。
 * @param recoveredToLsn redo reader 扫描到的最后完整 LSN。
 * @param repairedPageCount doublewrite 实际修复页数。
 * @param detectedOnlyPageCount doublewrite detect-only 发现但未修复的可疑页数。
 * @param appliedBatchCount 交给 redo dispatcher 的批次数。
 * @param completedStages 已完成阶段顺序。
 * @param skippedSpaces force-skip 模式显式跳过的表空间集合；普通模式为空。
 * @param skippedDoublewritePageCount doublewrite 阶段跳过的 page 数。
 * @param skippedRedoRecordCount redo replay 阶段跳过的 page record 数。
 * @param skippedReconcileSpaceCount SPACE_FILE_RECONCILE 阶段跳过的表空间数。
 */
public record RecoveryReport(RecoveryMode mode,
                             RecoveryState state,
                             Lsn checkpointLsn,
                             Lsn recoveredToLsn,
                             int repairedPageCount,
                             int detectedOnlyPageCount,
                             int appliedBatchCount,
                             List<RecoveryStageName> completedStages,
                             Set<SpaceId> skippedSpaces,
                             int skippedDoublewritePageCount,
                             int skippedRedoRecordCount,
                             int skippedReconcileSpaceCount) {

    public RecoveryReport {
        if (mode == null || state == null || checkpointLsn == null
                || recoveredToLsn == null || completedStages == null || skippedSpaces == null) {
            throw new DatabaseValidationException("recovery report fields must not be null");
        }
        if (repairedPageCount < 0 || detectedOnlyPageCount < 0 || appliedBatchCount < 0) {
            throw new DatabaseValidationException("recovery report counts must not be negative");
        }
        if (skippedDoublewritePageCount < 0 || skippedRedoRecordCount < 0 || skippedReconcileSpaceCount < 0) {
            throw new DatabaseValidationException("recovery skipped counts must not be negative");
        }
        completedStages = List.copyOf(completedStages);
        skippedSpaces = Set.copyOf(skippedSpaces);
    }

    /**
     * 兼容普通恢复报告构造。旧调用点不携带 skip 诊断，等价于 NORMAL/READ_ONLY 的空跳过集合。
     */
    public RecoveryReport(RecoveryMode mode,
                          RecoveryState state,
                          Lsn checkpointLsn,
                          Lsn recoveredToLsn,
                          int repairedPageCount,
                          int detectedOnlyPageCount,
                          int appliedBatchCount,
                          List<RecoveryStageName> completedStages) {
        this(mode, state, checkpointLsn, recoveredToLsn, repairedPageCount, detectedOnlyPageCount,
                appliedBatchCount, completedStages, Set.of(), 0, 0, 0);
    }

    /**
     * 构造普通可写恢复报告。
     *
     * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param checkpointLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param repairedPageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param detectedOnlyPageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param appliedBatchCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param completedStages 参与 {@code normal} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code normal} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public static RecoveryReport normal(RecoveryState state, Lsn checkpointLsn, Lsn recoveredToLsn,
                                        int repairedPageCount, int detectedOnlyPageCount,
                                        int appliedBatchCount, List<RecoveryStageName> completedStages) {
        return new RecoveryReport(RecoveryMode.NORMAL, state, checkpointLsn, recoveredToLsn,
                repairedPageCount, detectedOnlyPageCount, appliedBatchCount, completedStages,
                Set.of(), 0, 0, 0);
    }

    /**
     * 构造只读诊断恢复报告。该模式不应用 redo，也不产生 skipped-space 语义。
     *
     * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param checkpointLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param detectedOnlyPageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param completedStages 参与 {@code readOnlyValidate} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code readOnlyValidate} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public static RecoveryReport readOnlyValidate(RecoveryState state, Lsn checkpointLsn, Lsn recoveredToLsn,
                                                  int detectedOnlyPageCount,
                                                  List<RecoveryStageName> completedStages) {
        return new RecoveryReport(RecoveryMode.READ_ONLY_VALIDATE, state, checkpointLsn, recoveredToLsn,
                0, detectedOnlyPageCount, 0, completedStages, Set.of(), 0, 0, 0);
    }

    /**
     * 构造 FORCE_SKIP_CORRUPT_TABLESPACE 恢复报告，显式记录每个跳过阶段的诊断计数。
     *
     * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param checkpointLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param repairedPageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param detectedOnlyPageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param appliedBatchCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param skippedSpaces 参与 {@code forceSkip} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param skippedDoublewritePageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param skippedRedoRecordCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param skippedReconcileSpaceCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param completedStages 参与 {@code forceSkip} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code forceSkip} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public static RecoveryReport forceSkip(RecoveryState state, Lsn checkpointLsn, Lsn recoveredToLsn,
                                           int repairedPageCount, int detectedOnlyPageCount,
                                           int appliedBatchCount, Set<SpaceId> skippedSpaces,
                                           int skippedDoublewritePageCount, int skippedRedoRecordCount,
                                           int skippedReconcileSpaceCount,
                                           List<RecoveryStageName> completedStages) {
        return new RecoveryReport(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE, state, checkpointLsn, recoveredToLsn,
                repairedPageCount, detectedOnlyPageCount, appliedBatchCount, completedStages,
                skippedSpaces, skippedDoublewritePageCount, skippedRedoRecordCount, skippedReconcileSpaceCount);
    }

    /**
     * 构造 fail-closed 报告。失败时可能尚未读出 checkpoint/redo 边界，因此 LSN 与计数均为 0。
     *
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param skippedSpaces 参与 {@code failed} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code failed} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public static RecoveryReport failed(RecoveryMode mode, Set<SpaceId> skippedSpaces) {
        return new RecoveryReport(mode, RecoveryState.FAILED, Lsn.of(0), Lsn.of(0),
                0, 0, 0, List.of(), skippedSpaces == null ? Set.of() : skippedSpaces, 0, 0, 0);
    }
}
