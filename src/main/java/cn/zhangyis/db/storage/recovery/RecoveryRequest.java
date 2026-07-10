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
 * @param transactionRecoveryContext 可选正式事务恢复上下文；在 redo replay 前装载 sidecar，期间接收 transaction delta。
 * @param transactionUndoRecovery 可选事务 undo 恢复参与者；消费 immutable snapshot 并负责正式 UNDO_ROLLBACK/RESUME_PURGE。
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
                              TransactionRecoveryContext transactionRecoveryContext,
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
        if (transactionUndoRecovery != null && transactionRecoveryContext == null) {
            throw new DatabaseValidationException(
                    "transaction undo recovery requires a formal transaction recovery context");
        }
        if (mode == RecoveryMode.READ_ONLY_VALIDATE && transactionUndoRecovery != null) {
            throw new DatabaseValidationException(
                    "READ_ONLY_VALIDATE cannot execute transaction undo recovery");
        }
        if (mode != RecoveryMode.READ_ONLY_VALIDATE
                && transactionRecoveryContext != null && transactionUndoRecovery == null) {
            throw new DatabaseValidationException(
                    "writable transaction recovery context requires an undo recovery participant");
        }
        if (mode != RecoveryMode.READ_ONLY_VALIDATE && transactionRecoveryContext != null
                && !dispatcher.isBoundToTransactionStateSink(transactionRecoveryContext.deltaSink())) {
            throw new DatabaseValidationException(
                    "formal transaction recovery context requires its bound transaction-state dispatcher");
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
                dispatcher, applyContext, null, List.of(), null, List.of(), null, null, null,
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
                dispatcher, applyContext, null, List.of(), null, List.of(), null, null, null,
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
                null, List.of(), null, List.of(), null, null, null,
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
                recoveredRedoManager, transactionRecoveryContext, transactionUndoRecovery, skipPolicy);
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
                recoveredRedoManager, transactionRecoveryContext, transactionUndoRecovery, skipPolicy);
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
                recoveredRedoManager, transactionRecoveryContext, transactionUndoRecovery, skipPolicy);
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
                redoManager, transactionRecoveryContext, transactionUndoRecovery, skipPolicy);
    }

    /**
     * 接入正式事务表恢复：context 同时提供 sidecar 基线和 dispatcher sink，participant 只接收 replay 后快照。
     * 本方法重建标准 page dispatcher 并绑定该 context 的 sink，调用方先前传入的 no-op dispatcher 不会静默丢 delta。
     *
     * @param context 事务恢复上下文。
     * @param participant page3/undo 恢复参与者。
     * @return 携带正式事务恢复链的新请求。
     */
    public RecoveryRequest withTransactionRecovery(
            TransactionRecoveryContext context, TransactionUndoRecoveryParticipant participant) {
        if (context == null || participant == null) {
            throw new DatabaseValidationException("transaction recovery context/participant must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository,
                RedoApplyDispatcher.pageDispatcher(context.deltaSink()), applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spacesToReconcile,
                recoveredRedoManager, context, participant, skipPolicy);
    }

    /**
     * READ_ONLY_VALIDATE 只接事务 sidecar 覆盖校验，不注入 redo sink 或 undo participant，保证诊断路径不写页/redo。
     */
    public RecoveryRequest withTransactionRecoveryValidation(TransactionRecoveryContext context) {
        if (context == null) {
            throw new DatabaseValidationException("transaction recovery validation context must not be null");
        }
        if (mode != RecoveryMode.READ_ONLY_VALIDATE) {
            throw new DatabaseValidationException(
                    "transaction recovery validation-only context requires READ_ONLY_VALIDATE mode");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spacesToReconcile,
                recoveredRedoManager, context, transactionUndoRecovery, skipPolicy);
    }
}
