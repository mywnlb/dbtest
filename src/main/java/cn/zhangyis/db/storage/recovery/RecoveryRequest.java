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
 * @param skipPolicy 对象级恢复排除策略；NORMAL/READ_ONLY 可携带 committed DD 隔离集合，但管理员集合仅 FORCE 可用。
 * @param changeBufferRecovery 可选 redo 后 Change Buffer 持久结构校验参与者；只读诊断同样执行只读校验
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
                              RecoverySpaceExclusionPolicy skipPolicy,
                              ChangeBufferRecoveryParticipant changeBufferRecovery) {

    public RecoveryRequest {
        if (mode == null || checkpointStore == null || redoRepository == null
                || dispatcher == null || applyContext == null) {
            throw new DatabaseValidationException("recovery request core dependencies must not be null");
        }
        if (skipPolicy == null) {
            skipPolicy = RecoverySpaceExclusionPolicy.none();
        }
        if (mode != RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE
                && !skipPolicy.administrativeSpaces().isEmpty()) {
            throw new DatabaseValidationException(
                    "administrative recovery exclusions are only allowed in FORCE_SKIP_CORRUPT_TABLESPACE mode");
        }
        if (mode == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE
                && skipPolicy.administrativeSpaces().isEmpty()) {
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

    /** 兼容 Change Buffer 恢复阶段引入前的 RecoverySpaceExclusionPolicy 构造调用。 */
    public RecoveryRequest(RecoveryMode mode,
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
                           RecoverySpaceExclusionPolicy skipPolicy) {
        this(mode, checkpointStore, redoRepository, dispatcher, applyContext, doublewriteScanner,
                pagesToRepair, undoTablespaceRecovery, spacesToReconcile, recoveredRedoManager,
                transactionRecoveryContext, transactionUndoRecovery, skipPolicy, null);
    }

    /** 兼容仍按旧单集合模型构造请求的低层测试；该集合被解释为管理员本次声明。 */
    public RecoveryRequest(RecoveryMode mode,
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
        this(mode, checkpointStore, redoRepository, dispatcher, applyContext, doublewriteScanner,
                pagesToRepair, undoTablespaceRecovery, spacesToReconcile, recoveredRedoManager,
                transactionRecoveryContext, transactionUndoRecovery,
                skipPolicy == null ? RecoverySpaceExclusionPolicy.none()
                        : RecoverySpaceExclusionPolicy.of(skipPolicy.skippedSpaces(), Set.of()), null);
    }

    /**
     * 创建 NORMAL 模式请求。默认不检查 doublewrite 页，适合纯 redo replay 测试或后续 discovery 之前的空扫描。
     *
     * @param checkpointStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param redoRepository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param dispatcher 由组合根提供的 {@code RedoApplyDispatcher} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code normal} 调用
     * @param applyContext redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code normal} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     */
    public static RecoveryRequest normal(RedoCheckpointStore checkpointStore,
                                         RedoLogFileRepository redoRepository,
                                         RedoApplyDispatcher dispatcher,
                                         RedoApplyContext applyContext) {
        return new RecoveryRequest(RecoveryMode.NORMAL, checkpointStore, redoRepository,
                dispatcher, applyContext, null, List.of(), null, List.of(), null, null, null,
                RecoverySpaceExclusionPolicy.none());
    }

    /**
     * 创建 READ_ONLY_VALIDATE 模式请求。该模式复用 redo reader 和 doublewrite scanner 输入，但恢复服务只能扫描并报告，
     * 不能执行 page apply、redo 边界安装、undo 续作、空间 reconcile 或事务 rollback。
     *
     * @param checkpointStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param redoRepository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param dispatcher 由组合根提供的 {@code RedoApplyDispatcher} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code readOnlyValidate} 调用
     * @param applyContext redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code readOnlyValidate} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     */
    public static RecoveryRequest readOnlyValidate(RedoCheckpointStore checkpointStore,
                                                   RedoLogFileRepository redoRepository,
                                                   RedoApplyDispatcher dispatcher,
                                                   RedoApplyContext applyContext) {
        return new RecoveryRequest(RecoveryMode.READ_ONLY_VALIDATE, checkpointStore, redoRepository,
                dispatcher, applyContext, null, List.of(), null, List.of(), null, null, null,
                RecoverySpaceExclusionPolicy.none());
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
                RecoverySpaceExclusionPolicy.of(skippedSpaces, Set.of()));
    }

    /**
     * 把组合根已从 committed DD 证明的隔离集合带入所有恢复模式；保持其它恢复输入逐字不变。
     *
     * @param policy 已保留管理员/DD 来源的完整对象级排除策略
     * @return 携带同一恢复依赖和新排除证据的不可变请求
     */
    public RecoveryRequest withSpaceExclusionPolicy(RecoverySpaceExclusionPolicy policy) {
        if (policy == null) {
            throw new DatabaseValidationException("recovery space exclusion policy must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spacesToReconcile,
                recoveredRedoManager, transactionRecoveryContext, transactionUndoRecovery, policy,
                changeBufferRecovery);
    }

    /**
     * 返回带 doublewrite repair 阶段输入的新请求。保持请求不可变，避免启动恢复过程中外部修改页列表。
     *
     * @param scanner 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @param pages 参与 {@code withDoublewriteRepair} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code withDoublewriteRepair} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecoveryRequest withDoublewriteRepair(DoublewriteRecoveryScanner scanner, List<PageId> pages) {
        if (scanner == null) {
            throw new DatabaseValidationException("doublewrite recovery scanner must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                scanner, pages, undoTablespaceRecovery, spacesToReconcile,
                recoveredRedoManager, transactionRecoveryContext, transactionUndoRecovery, skipPolicy,
                changeBufferRecovery);
    }

    /**
     * 接入 undo TRUNCATING 启动恢复。参与者持有显式配置空间集及生命周期服务，request 本身保持不可变。
     *
     * @param participant 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code withUndoTablespaceRecovery} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecoveryRequest withUndoTablespaceRecovery(UndoTablespaceRecoveryParticipant participant) {
        if (participant == null) {
            throw new DatabaseValidationException("undo tablespace recovery participant must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, participant, spacesToReconcile,
                recoveredRedoManager, transactionRecoveryContext, transactionUndoRecovery, skipPolicy,
                changeBufferRecovery);
    }

    /**
     * 接入 SPACE_FILE_RECONCILE 阶段：传入需要把物理文件大小重对齐到 page0 权威逻辑大小的表空间集。
     * 保持请求不可变，避免启动恢复过程中外部修改空间集。
     *
     * @param spaces 待重对齐的表空间集（不可变快照）。
     * @return 携带空间集的新请求。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecoveryRequest withSpaceFileReconcile(List<SpaceId> spaces) {
        if (spaces == null) {
            throw new DatabaseValidationException("space file reconcile set must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spaces,
                recoveredRedoManager, transactionRecoveryContext, transactionUndoRecovery, skipPolicy,
                changeBufferRecovery);
    }

    /**
     * 接入 REDO_BOUNDARY_INSTALL 阶段：传入本进程将继续 append 的 RedoLogManager，redo replay 后把恢复边界
     * 安装到它，使新 MTR 从 recoveredToLsn 续写。真实重启恢复必须调用，否则新日志会从 0 覆盖历史。
     *
     * @param redoManager 恢复后继续使用的 redo 管理器（须尚未 append）。
     * @return 携带 redo 管理器的新请求。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecoveryRequest withRedoBoundaryInstall(RedoLogManager redoManager) {
        if (redoManager == null) {
            throw new DatabaseValidationException("recovered redo manager must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spacesToReconcile,
                redoManager, transactionRecoveryContext, transactionUndoRecovery, skipPolicy,
                changeBufferRecovery);
    }

    /**
     * 接入正式事务表恢复：context 同时提供 sidecar 基线和 dispatcher sink，participant 只接收 replay 后快照。
     * 本方法重建标准 page dispatcher 并绑定该 context 的 sink，调用方先前传入的 no-op dispatcher 不会静默丢 delta。
     *
     * @param context 事务恢复上下文。
     * @param participant page3/undo 恢复参与者。
     * @return 携带正式事务恢复链的新请求。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecoveryRequest withTransactionRecovery(
            TransactionRecoveryContext context, TransactionUndoRecoveryParticipant participant) {
        if (context == null || participant == null) {
            throw new DatabaseValidationException("transaction recovery context/participant must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository,
                RedoApplyDispatcher.pageDispatcher(context.deltaSink()), applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spacesToReconcile,
                recoveredRedoManager, context, participant, skipPolicy, changeBufferRecovery);
    }

    /**
     * READ_ONLY_VALIDATE 只接事务 sidecar 覆盖校验，不注入 redo sink 或 undo participant，保证诊断路径不写页/redo。
     *
     * @param context 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @return {@code withTransactionRecoveryValidation} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
                recoveredRedoManager, context, transactionUndoRecovery, skipPolicy,
                changeBufferRecovery);
    }

    /**
     * 接入 redo replay 后、undo rollback 前的 Change Buffer 只读校验阶段。
     *
     * @param participant 与当前 system.ibd/Buffer Pool/MTR 组合根一致的恢复参与者
     * @return 保留其它恢复输入并携带参与者的新请求
     */
    public RecoveryRequest withChangeBufferRecovery(ChangeBufferRecoveryParticipant participant) {
        if (participant == null) {
            throw new DatabaseValidationException("change buffer recovery participant must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
                doublewriteScanner, pagesToRepair, undoTablespaceRecovery, spacesToReconcile,
                recoveredRedoManager, transactionRecoveryContext, transactionUndoRecovery, skipPolicy,
                participant);
    }
}
