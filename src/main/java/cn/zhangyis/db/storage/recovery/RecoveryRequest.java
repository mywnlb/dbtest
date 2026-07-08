package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;

import java.util.List;
import java.util.Set;

/**
 * R2 crash recovery 输入。它显式携带 redo、checkpoint、doublewrite 和 page apply 依赖，避免 Recovery 模块读取各模块内部状态。
 *
 * @param mode 恢复模式。
 * @param checkpointStore redo control checkpoint label 仓储。
 * @param redoRepository redo data file 仓储。
 * @param dispatcher redo apply 分发器。
 * @param applyContext redo page apply 上下文。
 * @param doublewriteScanner 可选 doublewrite 修复器；为空表示跳过该阶段的实际页修复。
 * @param pagesToRepair R2 显式传入的待检查页；后续 tablespace discovery 会替代该列表来源。
 * @param undoTablespaceRecovery 可选 undo TRUNCATING 恢复参与者；其内部只接受显式配置 SpaceId，不做 discovery。
 * @param spacesToReconcile SPACE_FILE_RECONCILE 阶段需要把物理文件大小重对齐到 page0 权威逻辑大小的表空间集；
 *                          空集表示跳过该阶段。同样只接受显式配置 SpaceId，不做 discovery。
 * @param recoveredRedoManager REDO_BOUNDARY_INSTALL 阶段使用的、本进程将继续 append 的 RedoLogManager；
 *                             为空表示跳过该阶段（仅用于不需要续写 redo 的纯回放测试）。真实重启必须提供，
 *                             否则新 MTR 会从 0 重新分配 LSN 覆盖已有日志。
 * @param transactionUndoRecovery 可选事务 undo 恢复参与者；负责正式 UNDO_ROLLBACK/RESUME_PURGE 阶段。
 * @param skipPolicy force-skip 恢复策略；NORMAL/READ_ONLY_VALIDATE 必须为空，避免普通启动隐式跳过数据。
 */
public record RecoveryRequest(RecoveryMode mode,
                              RedoCheckpointStore checkpointStore,
                              RedoLogFileRepository redoRepository,
                              RedoApplyDispatcher dispatcher,
                              RedoApplyContext applyContext,
                              DoublewriteRecoveryScanner doublewriteScanner,
                              List<PageId> pagesToRepair,
                              UndoTablespaceRecoveryParticipant undoTablespaceRecovery,
                              List<SpaceId> spacesToReconcile,
                              RedoLogManager recoveredRedoManager,
                              TransactionUndoRecoveryParticipant transactionUndoRecovery,
                              RecoverySkipPolicy skipPolicy) {

    public RecoveryRequest {
        if (mode == null || checkpointStore == null || redoRepository == null
                || dispatcher == null || applyContext == null) {
            throw new DatabaseValidationException("recovery request core dependencies must not be null");
        }
        if (skipPolicy == null) {
            skipPolicy = RecoverySkipPolicy.none();
        }
        if (mode != RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE && !skipPolicy.isEmpty()) {
            throw new DatabaseValidationException(
                    "skip policy is only allowed in FORCE_SKIP_CORRUPT_TABLESPACE mode");
        }
        if (mode == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE && skipPolicy.isEmpty()) {
            throw new DatabaseValidationException("FORCE_SKIP_CORRUPT_TABLESPACE requires skipped spaces");
        }
        pagesToRepair = pagesToRepair == null ? List.of() : List.copyOf(pagesToRepair);
        if (doublewriteScanner == null && !pagesToRepair.isEmpty()) {
            throw new DatabaseValidationException("recovery pages require a doublewrite scanner");
        }
        spacesToReconcile = spacesToReconcile == null ? List.of() : List.copyOf(spacesToReconcile);
    }

    /**
     * 创建 NORMAL 模式请求。默认不检查 doublewrite 页，适合纯 redo replay 测试或后续 discovery 之前的空扫描。
     */
    public static RecoveryRequest normal(RedoCheckpointStore checkpointStore,
                                         RedoLogFileRepository redoRepository,
                                         RedoApplyDispatcher dispatcher,
                                         RedoApplyContext applyContext) {
        return new RecoveryRequest(RecoveryMode.NORMAL, checkpointStore, redoRepository,
                dispatcher, applyContext, null, List.of(), null, List.of(), null, null,
                RecoverySkipPolicy.none());
    }

    /**
     * 创建 READ_ONLY_VALIDATE 模式请求。该模式复用 redo reader 和 doublewrite scanner 输入，但恢复服务只能扫描并报告，
     * 不能执行 page apply、redo 边界安装、undo 续作、空间 reconcile 或事务 rollback。
     */
    public static RecoveryRequest readOnlyValidate(RedoCheckpointStore checkpointStore,
                                                   RedoLogFileRepository redoRepository,
                                                   RedoApplyDispatcher dispatcher,
                                                   RedoApplyContext applyContext) {
        return new RecoveryRequest(RecoveryMode.READ_ONLY_VALIDATE, checkpointStore, redoRepository,
                dispatcher, applyContext, null, List.of(), null, List.of(), null, null,
                RecoverySkipPolicy.none());
    }

    /**
     * 创建 FORCE_SKIP_CORRUPT_TABLESPACE 请求。该模式必须由调用方显式传入非空 skippedSpaces；
     * 具体哪些系统空间不可跳过由 StorageEngine 结合实例配置校验。
     *
     * @param checkpointStore redo checkpoint 仓储。
     * @param redoRepository redo 文件仓储。
     * @param dispatcher redo apply 分发器。
     * @param applyContext page apply 上下文。
     * @param skippedSpaces 管理员显式声明要跳过的表空间集合。
     * @return force-skip 恢复请求。
     */
    public static RecoveryRequest forceSkip(RedoCheckpointStore checkpointStore,
                                            RedoLogFileRepository redoRepository,
                                            RedoApplyDispatcher dispatcher,
                                            RedoApplyContext applyContext,
                                            Set<SpaceId> skippedSpaces) {
        return new RecoveryRequest(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE,
                checkpointStore, redoRepository, dispatcher, applyContext,
                null, List.of(), null, List.of(), null, null,
                RecoverySkipPolicy.of(skippedSpaces));
    }

    /**
     * 返回带 doublewrite repair 阶段输入的新请求。保持请求不可变，避免启动恢复过程中外部修改页列表。
     */
    public RecoveryRequest withDoublewriteRepair(DoublewriteRecoveryScanner scanner, List<PageId> pages) {
        if (scanner == null) {
            throw new DatabaseValidationException("doublewrite recovery scanner must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                scanner, pages, undoTablespaceRecovery, spacesToReconcile,
                recoveredRedoManager, transactionUndoRecovery, skipPolicy);
    }

    /**
     * 接入 undo TRUNCATING 启动恢复。参与者持有显式配置空间集及生命周期服务，request 本身保持不可变。
     */
    public RecoveryRequest withUndoTablespaceRecovery(UndoTablespaceRecoveryParticipant participant) {
        if (participant == null) {
            throw new DatabaseValidationException("undo tablespace recovery participant must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, participant, spacesToReconcile,
                recoveredRedoManager, transactionUndoRecovery, skipPolicy);
    }

    /**
     * 接入 SPACE_FILE_RECONCILE 阶段：传入需要把物理文件大小重对齐到 page0 权威逻辑大小的表空间集。
     * 保持请求不可变，避免启动恢复过程中外部修改空间集。
     *
     * @param spaces 待重对齐的表空间集（不可变快照）。
     * @return 携带空间集的新请求。
     */
    public RecoveryRequest withSpaceFileReconcile(List<SpaceId> spaces) {
        if (spaces == null) {
            throw new DatabaseValidationException("space file reconcile set must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spaces,
                recoveredRedoManager, transactionUndoRecovery, skipPolicy);
    }

    /**
     * 接入 REDO_BOUNDARY_INSTALL 阶段：传入本进程将继续 append 的 RedoLogManager，redo replay 后把恢复边界
     * 安装到它，使新 MTR 从 recoveredToLsn 续写。真实重启恢复必须调用，否则新日志会从 0 覆盖历史。
     *
     * @param redoManager 恢复后继续使用的 redo 管理器（须尚未 append）。
     * @return 携带 redo 管理器的新请求。
     */
    public RecoveryRequest withRedoBoundaryInstall(RedoLogManager redoManager) {
        if (redoManager == null) {
            throw new DatabaseValidationException("recovered redo manager must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spacesToReconcile,
                redoManager, transactionUndoRecovery, skipPolicy);
    }

    /**
     * 接入正式事务 undo 恢复阶段：redo replay 和物理空间续作完成后，OPEN_TRAFFIC 前调用该参与者扫描 page3、
     * rollback recovered ACTIVE 段并重建 committed history。request 保持不可变，避免启动恢复中替换参与者。
     *
     * @param participant 事务 undo 恢复参与者。
     * @return 携带事务 undo 恢复参与者的新请求。
     */
    public RecoveryRequest withTransactionUndoRecovery(TransactionUndoRecoveryParticipant participant) {
        if (participant == null) {
            throw new DatabaseValidationException("transaction undo recovery participant must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spacesToReconcile,
                recoveredRedoManager, participant, skipPolicy);
    }
}
