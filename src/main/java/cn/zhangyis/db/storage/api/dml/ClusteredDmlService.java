package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadMode;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadRequest;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadService;
import cn.zhangyis.db.storage.btree.BTreeDeleteMarkResult;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.BTreeUpdateResult;
import cn.zhangyis.db.storage.btree.BTreeUniqueCheckResult;
import cn.zhangyis.db.storage.btree.PreparedClusteredInsert;
import cn.zhangyis.db.storage.btree.PreparedClusteredUpdate;
import cn.zhangyis.db.storage.btree.PreparedUpdateStateException;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.api.lob.LobStorage;
import cn.zhangyis.db.storage.api.lob.LobWriteAllocation;
import cn.zhangyis.db.storage.api.lob.LobWritePlan;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.recovery.RecoveryState;
import cn.zhangyis.db.storage.recovery.RecoveryTrafficGate;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.schema.StorageKind;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.trx.EmptyUndoBoundary;
import cn.zhangyis.db.storage.trx.RollbackService;
import cn.zhangyis.db.storage.trx.RollbackSummary;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.DeferredInsertUndoPlan;
import cn.zhangyis.db.storage.trx.DeferredUpdateUndoPlan;
import cn.zhangyis.db.storage.trx.PreparedUndoAppend;
import cn.zhangyis.db.storage.trx.UndoWriteFatalException;
import cn.zhangyis.db.storage.trx.UndoWritePlan;
import cn.zhangyis.db.storage.trx.TransactionSavepoint;
import cn.zhangyis.db.storage.trx.TransactionState;
import cn.zhangyis.db.storage.trx.TransactionStateException;
import cn.zhangyis.db.storage.trx.UndoLogManager;
import cn.zhangyis.db.storage.trx.lock.LockManager;
import cn.zhangyis.db.storage.undo.InsertedLobOwnership;
import cn.zhangyis.db.storage.undo.LobVersionOwnership;
import cn.zhangyis.db.storage.undo.SecondaryUndoMutation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 单表/单聚簇索引 DML 内核 facade。生产 SQL INSERT 由 {@link TableDmlService} 编排全部索引后复用本类作为聚簇 undo anchor；
 * 低层调用方仍可显式传入事务、
 * 聚簇索引快照和已归一化记录，本服务只编排 transaction、undo、current-read、B+Tree、redo durability
 * 与 row-lock release，不解析 SQL、不访问 BufferFrame/RecordCursor/裸文件。
 */
public final class ClusteredDmlService {

    /** 授锁后重定位最多重试次数；current-read 下页 latch 已释放，重试用于处理结构变化或锁落点变化。 */
    private static final int DEFAULT_RELOCATION_RETRIES = 3;

    /** 事务状态机与写 id / transaction no 分配来源。 */
    private final TransactionManager transactionManager;
    /** undo 写入与 commit history/reclaim 编排入口。 */
    private final UndoLogManager undoLogManager;
    /** 业务写 MTR 工厂；每条聚簇记录修改使用一个短 MTR。 */
    private final MiniTransactionManager mtrManager;
    /** 聚簇 B+Tree 写入原语；只接收值对象，不依赖事务/SQL。 */
    private final SplitCapableBTreeIndexService btree;
    /** current-read 协调器；所有可能等待 row lock 的路径必须经它释放 page latch 后等待。 */
    private final BTreeCurrentReadService currentRead;
    /** full transaction rollback 执行器；按 undo 链反向恢复聚簇记录。 */
    private final RollbackService rollbackService;
    /** 事务 row-lock 真相来源；commit/rollback 收尾释放事务持锁。 */
    private final LockManager lockManager;
    /** redo 持久化边界；commit durability policy 等待该 manager 的 write/flush 状态。 */
    private final RedoLogManager redo;
    /** 启动恢复流量门控；非 OPEN 时拒绝普通 DML 进入。 */
    private final RecoveryTrafficGate recoveryGate;
    /** LOB 纯规划、segment purpose 预检、同 MTR allocation 与异常补偿入口。 */
    private final LobStorage lobStorage;

    /**
     * 构造 DML facade。所有 collaborator 都来自 {@code StorageEngine} 组合根，保证 DML 与测试/恢复/后台 purge
     * 使用同一套事务、undo、锁和 redo 状态，而不是另建旁路实例。
     * @param transactionManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param undoLogManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param mtrManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param btree 由组合根提供的 {@code SplitCapableBTreeIndexService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param currentRead 由组合根提供的 {@code BTreeCurrentReadService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param rollbackService 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param lockManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param recoveryGate 由组合根提供的 {@code RecoveryTrafficGate} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param lobStorage 由组合根提供的 {@code LobStorage} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public ClusteredDmlService(TransactionManager transactionManager, UndoLogManager undoLogManager,
                               MiniTransactionManager mtrManager, SplitCapableBTreeIndexService btree,
                               BTreeCurrentReadService currentRead, RollbackService rollbackService,
                               LockManager lockManager, RedoLogManager redo, RecoveryTrafficGate recoveryGate,
                               LobStorage lobStorage) {
        if (transactionManager == null || undoLogManager == null || mtrManager == null || btree == null
                || currentRead == null || rollbackService == null || lockManager == null
                || redo == null || recoveryGate == null || lobStorage == null) {
            throw new DatabaseValidationException("clustered DML service collaborators must not be null");
        }
        this.transactionManager = transactionManager;
        this.undoLogManager = undoLogManager;
        this.mtrManager = mtrManager;
        this.btree = btree;
        this.currentRead = currentRead;
        this.rollbackService = rollbackService;
        this.lockManager = lockManager;
        this.redo = redo;
        this.recoveryGate = recoveryGate;
        this.lobStorage = lobStorage;
    }

    /**
     * 打开一个显式 DML statement 边界。该 API 只固定 storage 层 undo 边界，不自动包裹单行
     * {@link #insert(ClusteredInsertCommand)}/{@link #update(ClusteredUpdateCommand)}/
     * {@link #delete(ClusteredDeleteCommand)}；上层 executor 因而可以用一个 Guard 覆盖多行修改。
     *
     * <p>事务已有 undo context 时创建真实保存点；事务尚未首写时创建一次性空 undo 边界令牌，后续失败可撤销
     * 语句内首写。
     * 调用方在失败分支必须先调用 {@link DmlStatementGuard#rollback()}，成功分支才直接
     * {@link DmlStatementGuard#close()}。partial rollback 不结束事务，也不释放事务级 row locks、ReadView 或
     * undo slot；SQL/session 自动 statement 生命周期与命名 SAVEPOINT 不在本 v1 范围内。
     *
     * @param txn            当前 ACTIVE 事务，不能为 null。
     * @param clusteredIndex 本语句写入的单一聚簇索引，不能为 null。
     * @return 绑定到当前 undo 边界的一次性 statement guard。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public DmlStatementGuard beginStatement(Transaction txn, BTreeIndex clusteredIndex) {
        if (txn == null || clusteredIndex == null) {
            throw new DatabaseValidationException("DML statement txn/index must not be null");
        }
        requireOpenForDml();
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("DML statement requires ACTIVE transaction: " + txn.state());
        }
        if (txn.undoContext() == null) {
            EmptyUndoBoundary boundary = rollbackService.createEmptyStatementBoundary(txn);
            return DmlStatementGuard.emptyBoundary(rollbackService, txn, clusteredIndex, boundary);
        }
        TransactionSavepoint savepoint = rollbackService.createSavepoint(txn);
        return DmlStatementGuard.savepointBoundary(rollbackService, txn, clusteredIndex, savepoint);
    }

    /**
     * 执行不携带二级反向证据的单聚簇索引 INSERT；兼容现有 storage API 调用，磁盘 secondary tail 保持为空。
     *
     * @param command ACTIVE 事务、聚簇 descriptor、完整主键/行、表 id、可选 LOB segment 与锁等待边界。
     * @return 插入影响行数、业务 MTR end LSN 与事务 write id。
     * @throws DatabaseValidationException 命令或内部字段无效时抛出。
     * @throws DmlDuplicateKeyException 聚簇完整主键已被占用时抛出。
     * @throws DatabaseRuntimeException 锁等待、undo/LOB/B+Tree/MTR 或 redo 发布失败时抛出。
     */
    public DmlWriteResult insert(ClusteredInsertCommand command) {
        return insert(command, List.of());
    }

    /**
     * 表级 DML 内部入口：聚簇写与二级发布仍分属多个短 MTR，但首个聚簇 MTR 必须把完整 secondary inverse
     * 列表写进同一逻辑 INSERT undo。仅同包 {@link TableDmlService} 调用；普通单聚簇入口传空列表保持旧磁盘编码。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验命令/secondary mutation 列表和引擎状态，分配 write id，并在无页 latch 时完成聚簇唯一
     *         current-read；重复键在创建 undo/MTR 前失败。</li>
     *     <li>规划 LOB inline/external 表示、预检 LOB segment，冻结含 secondary tail 的 deferred INSERT undo，
     *         再以 undo + B+Tree + LOB 总 workload 申请 redo admission。</li>
     *     <li>在同一业务 MTR 中 prepare undo append 与聚簇插入槽位；prepare 后失败必须按物理边界规则升级为 fatal。</li>
     *     <li>写入计划内 LOB 页，把实际 ownership 追加到 undo，再以真实 ExternalValue 和 roll pointer 发布聚簇行；
     *         ownership 只在聚簇记录可达后转移。</li>
     *     <li>关闭 guard、提交 MTR 并返回；异常时逆序补偿未转移 LOB/插入槽/undo append，回滚 ACTIVE MTR，
     *         且保留原始 cause 与 prepared-boundary 安全语义。</li>
     * </ol>
     *
     * @param command            表级 INSERT 已完成基本 schema 校验的聚簇命令。
     * @param secondaryMutations 按 index id 严格递增的 INSERT_ENTRY 反向证据；构造 undo 时防御性复制。
     * @return 成功插入一行的结果，包含业务 MTR end LSN 和稳定 write id。
     * @throws DatabaseValidationException 参数缺失、引擎状态或 mutation/undo 组合无效时抛出。
     * @throws DmlDuplicateKeyException 聚簇主键已存在时抛出。
     * @throws UndoWriteFatalException prepare 物理边界或 undo 已写入后后续步骤失败、无法安全继续事务时抛出。
     * @throws DatabaseRuntimeException 锁、LOB、B+Tree、MTR 或 redo 失败时抛出并保留底层 cause。
     * @throws DmlOperationException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    DmlWriteResult insert(ClusteredInsertCommand command,
                          List<SecondaryUndoMutation> secondaryMutations) {
        // 1. 先完成纯校验和事务锁 current-read；重复键分支尚未创建 MTR、undo slot 或 LOB allocation。
        if (command == null) {
            throw new DatabaseValidationException("clustered insert command must not be null");
        }
        if (secondaryMutations == null) {
            throw new DatabaseValidationException("clustered insert secondary mutations must not be null");
        }
        requireOpenForDml();
        Transaction txn = command.transaction();
        TransactionId txnId = transactionManager.assignWriteId(txn);
        BTreeCurrentReadRequest request = currentReadRequest(txn, txnId, command.lockWaitTimeout());
        BTreeUniqueCheckResult unique = currentRead.checkUniqueForInsert(command.index(), command.key(), request);
        if (unique.duplicate()) {
            throw new DmlDuplicateKeyException("duplicate clustered key for index " + command.index().indexId());
        }

        // 2. 冻结 LOB、undo secondary tail 与总 redo workload，admission 成功后才接触可写页。
        PlannedInsertLobs plannedLobs = planInsertLobs(command);
        preflightLobSegment(command.lobSegment(), !plannedLobs.values().isEmpty());
        DeferredInsertUndoPlan undoPlan = undoLogManager.planDeferredInsert(txn, command.tableId(),
                command.index().indexId(), command.key().values(), plannedLobs.placeholderOwnerships(),
                secondaryMutations, command.index().keyDef(), command.index().schema());
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.CLUSTERED_INSERT,
                DmlRedoBudgetEstimator.insert(command.index(), undoPlan, plannedLobs.writePlans())));
        PreparedUndoAppend<InsertedLobOwnership> preparedUndo = null;
        PreparedClusteredInsert preparedInsert = null;
        List<LobWriteAllocation> allocations = new ArrayList<>(plannedLobs.values().size());
        boolean preparedBoundary = false;
        try {
            // 3. undo append 与聚簇插入先 prepare；两者的 guard 都由本方法在正常或异常路径显式关闭。
            preparedUndo = undoLogManager.prepareUndoAppend(txn, mtr, undoPlan);
            preparedBoundary = true;
            preparedInsert = btree.prepareClusteredInsert(mtr, command.index(),
                    plannedLobs.placeholderRecord(), txnId);
            // 4. 外部 LOB 的真实引用先写入 undo ownership，再随 roll pointer 发布聚簇行，最后转移 allocation owner。
            List<ColumnValue> actualValues = new ArrayList<>(plannedLobs.placeholderRecord().columnValues());
            List<InsertedLobOwnership> actualOwnerships = new ArrayList<>(plannedLobs.values().size());
            for (PlannedLobValue planned : plannedLobs.values()) {
                LobWriteAllocation allocation = lobStorage.writePlanned(mtr, planned.plan());
                allocations.add(allocation);
                actualValues.set(planned.columnOrdinal(), allocation.value());
                actualOwnerships.add(new InsertedLobOwnership(planned.columnOrdinal(), allocation.value()));
            }
            LogicalRecord actualRecord = new LogicalRecord(command.record().schemaVersion(), actualValues,
                    command.record().deleted(), command.record().recordType(), command.record().hiddenColumns());
            RollPointer rollPointer = preparedUndo.appendActual(actualOwnerships);
            preparedInsert.publish(actualRecord, rollPointer);
            for (LobWriteAllocation allocation : allocations) {
                allocation.transferOwnership();
            }
            preparedInsert.close();
            preparedInsert = null;
            preparedUndo.close();
            preparedUndo = null;

            // 5. 所有 prepared guard 已关闭且 ownership 已转移后提交 MTR；返回的 LSN 覆盖本次物理原子组。
            Lsn endLsn = mtrManager.commit(mtr);
            return new DmlWriteResult(true, 1, endLsn, txnId);
        } catch (RuntimeException e) {
            // 5. 补偿按 allocation -> prepared insert -> prepared undo 逆序执行，再终止 ACTIVE MTR 并保持 cause。
            RuntimeException failure = compensateAndClosePrepared(
                    allocations, preparedInsert, preparedUndo, e);
            rollbackActiveMtr(mtr, failure);
            if (preparedBoundary && !(failure instanceof UndoWriteFatalException)) {
                throw new UndoWriteFatalException("clustered insert failed after prepare physical boundary", failure);
            }
            if (failure instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("clustered insert failed", failure);
        }
    }

    /**
     * 执行不携带二级反向证据的单聚簇索引 UPDATE；当前只支持不改变聚簇 key 的页内/重定位更新。
     *
     * @param command ACTIVE 事务、聚簇 descriptor、完整定位键、新完整行、表 id 与锁等待边界。
     * @return 目标不存在时零影响；命中时返回替换结果、业务 MTR end LSN 与事务 write id。
     * @throws DatabaseValidationException 命令或行/索引形状无效时抛出。
     * @throws DatabaseRuntimeException current-read、undo、B+Tree、MTR 或 redo 发布失败时抛出。
     */
    public DmlWriteResult update(ClusteredUpdateCommand command) {
        return update(command, List.of());
    }

    /**
     * 表级 UPDATE 的聚簇首 MTR 入口：把全部变键二级证据与聚簇旧 image 写入同一逻辑 undo。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验命令/mutation 与引擎状态，分配 write id，并用 FOR_UPDATE current-read 取得事务行锁和旧记录；
     *         miss 在任何 undo/MTR 写入前返回零影响。</li>
     *     <li>校验旧记录隐藏列，冻结全量旧 image 与 CHANGE_KEY secondary tail，并按 point rewrite 总 workload
     *         申请业务 MTR redo admission。</li>
     *     <li>先追加 UPDATE undo 取得新 roll pointer，再给新记录盖 write id/roll pointer，并以旧隐藏列作 CAS 证据替换聚簇版本。</li>
     *     <li>提交 MTR 并返回；异常时终止 ACTIVE MTR，若 undo 已物理写入则升级 fatal，避免事务继续产生不可逆分叉。</li>
     * </ol>
     *
     * @param command            已完成表级 current-read 前置规划的聚簇更新命令。
     * @param secondaryMutations 按 index id 递增的 CHANGE_KEY 反向证据；空列表保持单聚簇兼容语义。
     * @return miss 或成功替换的一行结果，包含业务 MTR end LSN/write id。
     * @throws DatabaseValidationException 参数、隐藏列、mutation 或引擎状态无效时抛出。
     * @throws UndoWriteFatalException undo 已写入后聚簇替换/提交失败时抛出。
     * @throws DatabaseRuntimeException current-read、redo admission、B+Tree 或 MTR 操作失败时抛出。
     * @throws DmlOperationException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    DmlWriteResult update(ClusteredUpdateCommand command,
                          List<SecondaryUndoMutation> secondaryMutations) {
        // 1. 所有可能阻塞的事务行锁等待发生在业务 MTR 前；miss 不分配 undo slot。
        if (command == null) {
            throw new DatabaseValidationException("clustered update command must not be null");
        }
        if (secondaryMutations == null) {
            throw new DatabaseValidationException("clustered update secondary mutations must not be null");
        }
        requireOpenForDml();
        Transaction txn = command.transaction();
        TransactionId txnId = transactionManager.assignWriteId(txn);
        BTreeCurrentReadRequest request = currentReadRequest(txn, txnId, command.lockWaitTimeout());
        Optional<BTreeLookupResult> locked = currentRead.lockPoint(command.index(), command.key(),
                request, BTreeCurrentReadMode.FOR_UPDATE);
        if (locked.isEmpty()) {
            return new DmlWriteResult(false, 0, redo.currentLsn(), txnId);
        }
        // 2. 旧隐藏列是版本 CAS 与 undo 链前驱；同时冻结 LOB replacement ownership、secondary tail 与 redo 上界。
        BTreeLookupResult old = locked.orElseThrow();
        HiddenColumns oldHidden = requireHiddenColumns(old.record(), "update");
        PlannedUpdateLobs plannedLobs = planUpdateLobs(command, old.record());
        preflightLobSegment(command.lobSegment(), plannedLobs.requiresLobSegment());
        if (!plannedLobs.values().isEmpty()) {
            return updateWithExternalLobs(command, secondaryMutations, txnId, old, oldHidden, plannedLobs);
        }

        UndoWritePlan undoPlan = undoLogManager.planUpdate(txn, command.tableId(), command.index().indexId(),
                command.key().values(), old.record().columnValues(), oldHidden,
                plannedLobs.placeholderOwnerships(), secondaryMutations,
                command.index().keyDef(), command.index().schema());
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.CLUSTERED_UPDATE,
                DmlRedoBudgetEstimator.pointRewrite(command.index(), undoPlan)));
        boolean undoWritten = false;
        try {
            // 3. undo 必须先于新聚簇版本发布，roll pointer 将两者连接成可恢复版本链。
            RollPointer rollPointer = undoLogManager.appendPlanned(txn, mtr, undoPlan);
            undoWritten = true;
            LogicalRecord stamped = stampedRecord(plannedLobs.placeholderRecord(), false,
                    new HiddenColumns(txnId, rollPointer));
            BTreeUpdateResult replaced = btree.replaceClustered(mtr, command.index(), command.key(),
                    stamped, oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            // 4. 聚簇替换与 undo 属于同一 MTR；提交失败时 catch 不允许事务在已写 undo 后继续运行。
            Lsn endLsn = mtrManager.commit(mtr);
            return new DmlWriteResult(replaced.replaced(), replaced.replaced() ? 1 : 0, endLsn, txnId);
        } catch (RuntimeException e) {
            // 4. 始终先释放 MTR memo；undo 已物理写入意味着普通可重试异常必须升级为 fatal。
            rollbackActiveMtr(mtr, e);
            if (undoWritten && !(e instanceof UndoWriteFatalException)) {
                throw new UndoWriteFatalException("clustered update failed after undo physical write", e);
            }
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("clustered update failed", e);
        }
    }

    /**
     * 执行至少包含一个新 external allocation 的聚簇 UPDATE prepared 协议。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>用 placeholder LV tail 冻结 deferred undo，并合并 point rewrite、undo 和全部 LOB write workload。</li>
     *     <li>依次 prepare undo append 与目标 clustered leaf，形成 index→LOB/FSP 的单向资源边界。</li>
     *     <li>写出所有新链，把真实 external envelope 同时替换进目标行和 rollback-new ownership。</li>
     *     <li>先发布 actual undo，再发布聚簇新版本，随后转移 allocation ownership 并关闭两个 prepared guard。</li>
     *     <li>提交业务 MTR；异常时按 allocation→clustered→undo 逆序补偿，prepare 后失败统一 fail-stop。</li>
     * </ol>
     *
     * @param command            当前 UPDATE 命令及 authoritative LOB segment。
     * @param secondaryMutations 表级调用冻结的 CHANGE_KEY 证据。
     * @param txnId              已分配的稳定 write id。
     * @param old                FOR_UPDATE current-read 物化的旧版本。
     * @param oldHidden          旧版本 CAS 与记录版本链证据。
     * @param plannedLobs        placeholder row、逐列 write plan 和 LV ownership 聚合。
     * @return 成功替换一行的 DML 结果。
     * @throws UndoWriteFatalException prepared owner/page、actual undo 或聚簇发布边界失败时抛出。
     * @throws DatabaseRuntimeException LOB allocation、redo、B+Tree 或 MTR 提交失败时抛出并保留 cause。
     * @throws PreparedUpdateStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     * @throws DmlOperationException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    private DmlWriteResult updateWithExternalLobs(ClusteredUpdateCommand command,
                                                  List<SecondaryUndoMutation> secondaryMutations,
                                                  TransactionId txnId, BTreeLookupResult old,
                                                  HiddenColumns oldHidden,
                                                  PlannedUpdateLobs plannedLobs) {
        // 1. placeholder 的 external envelope 与 actual 仅首页号不同，因此可在 begin 前冻结全部物理上界。
        DeferredUpdateUndoPlan undoPlan = undoLogManager.planDeferredUpdate(command.transaction(),
                command.tableId(), command.index().indexId(), command.key().values(),
                old.record().columnValues(), oldHidden, plannedLobs.placeholderOwnerships(),
                secondaryMutations, command.index().keyDef(), command.index().schema());
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.CLUSTERED_UPDATE,
                DmlRedoBudgetEstimator.pointRewrite(command.index(), undoPlan, plannedLobs.writePlans())));
        PreparedUndoAppend<LobVersionOwnership> preparedUndo = null;
        PreparedClusteredUpdate preparedUpdate = null;
        List<LobWriteAllocation> allocations = new ArrayList<>(plannedLobs.values().size());
        boolean preparedBoundary = false;
        try {
            // 2. undo owner/root 先于 index leaf prepare；两者固定后才允许访问 LOB/FSP。
            preparedUndo = undoLogManager.prepareUndoAppend(command.transaction(), mtr, undoPlan);
            preparedBoundary = true;
            preparedUpdate = btree.prepareClusteredUpdate(mtr, command.index(), command.key(),
                    plannedLobs.placeholderRecord(), txnId, oldHidden);

            // 3. 所有新链在同一 MTR 写出；actual external 同时进入目标行和 rollback ownership。
            List<ColumnValue> actualValues = new ArrayList<>(plannedLobs.placeholderRecord().columnValues());
            Map<Integer, ColumnValue.ExternalValue> actualByOrdinal = new HashMap<>();
            for (PlannedLobValue planned : plannedLobs.values()) {
                LobWriteAllocation allocation = lobStorage.writePlanned(mtr, planned.plan());
                allocations.add(allocation);
                actualValues.set(planned.columnOrdinal(), allocation.value());
                actualByOrdinal.put(planned.columnOrdinal(), allocation.value());
            }
            List<LobVersionOwnership> actualOwnerships = plannedLobs.placeholderOwnerships().stream()
                    .map(ownership -> ownership.rollbackNewValue().isEmpty() ? ownership
                            : new LobVersionOwnership(ownership.columnOrdinal(), ownership.purgeOldValue(),
                            Optional.of(actualByOrdinal.get(ownership.columnOrdinal()))))
                    .toList();
            LogicalRecord actualRecord = new LogicalRecord(command.newRecord().schemaVersion(), actualValues,
                    false, command.newRecord().recordType(), null);

            // 4. undo 必须先于行版本发布；allocation 只有在行和 undo 都可达后才转移 owner。
            RollPointer rollPointer = preparedUndo.appendActual(actualOwnerships);
            BTreeUpdateResult replaced = preparedUpdate.publish(actualRecord, rollPointer);
            if (!replaced.replaced()) {
                throw new PreparedUpdateStateException(
                        "prepared clustered UPDATE lost its target version before publication");
            }
            for (LobWriteAllocation allocation : allocations) allocation.transferOwnership();
            preparedUpdate.close();
            preparedUpdate = null;
            preparedUndo.close();
            preparedUndo = null;

            // 5. commit 返回后 redo/pageLSN/dirty 与 LOB/undo/row 形成同一 durable 原子组。
            Lsn endLsn = mtrManager.commit(mtr);
            return new DmlWriteResult(true, 1, endLsn, txnId);
        } catch (RuntimeException error) {
            RuntimeException failure = compensateAndClosePreparedUpdate(
                    allocations, preparedUpdate, preparedUndo, error);
            rollbackActiveMtr(mtr, failure);
            if (preparedBoundary && !(failure instanceof UndoWriteFatalException)) {
                throw new UndoWriteFatalException(
                        "clustered UPDATE failed after deferred prepare physical boundary", failure);
            }
            if (failure instanceof DatabaseRuntimeException databaseError) throw databaseError;
            throw new DmlOperationException("clustered LOB UPDATE failed", failure);
        }
    }

    /**
     * 执行不携带二级反向证据的单聚簇索引 DELETE；只发布 delete mark，物理删除留给 purge。
     *
     * @param command ACTIVE 事务、聚簇 descriptor、完整定位键、表 id 与锁等待边界。
     * @return 目标不存在时零影响；命中时返回标记结果、业务 MTR end LSN 与事务 write id。
     * @throws DatabaseValidationException 命令或索引/key 形状无效时抛出。
     * @throws DatabaseRuntimeException current-read、undo、B+Tree、MTR 或 redo 发布失败时抛出。
     */
    public DmlWriteResult delete(ClusteredDeleteCommand command) {
        return delete(command, List.of());
    }

    /**
     * 表级 DELETE 的聚簇首 MTR 入口：把全部二级 delete-mark inverse 与聚簇旧 image 写入同一逻辑 undo。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验命令/mutation 与引擎状态，分配 write id，并用 FOR_UPDATE current-read 取得事务行锁和旧记录；
     *         miss 在任何 undo/MTR 写入前返回零影响。</li>
     *     <li>读取删除前存活版本的隐藏列，冻结全量旧 image 与 DELETE_MARK_ENTRY tail，并申请 point rewrite redo admission。</li>
     *     <li>先追加 DELETE_MARK undo，再以 write id/新 roll pointer 和旧隐藏列 CAS 证据翻转聚簇记录 delete 位。</li>
     *     <li>提交 MTR 并返回；异常时终止 ACTIVE MTR，undo 已写入后的失败升级 fatal，且本方法不回收物理记录。</li>
     * </ol>
     *
     * @param command            已完成表级规划的聚簇删除命令。
     * @param secondaryMutations 按 index id 递增的 DELETE_MARK_ENTRY 反向证据；空列表保持单聚簇兼容语义。
     * @return miss 或成功标记的一行结果，包含业务 MTR end LSN/write id。
     * @throws DatabaseValidationException 参数、隐藏列、mutation 或引擎状态无效时抛出。
     * @throws UndoWriteFatalException undo 已写入后聚簇标记/提交失败时抛出。
     * @throws DatabaseRuntimeException current-read、redo admission、B+Tree 或 MTR 操作失败时抛出。
     * @throws DmlOperationException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    DmlWriteResult delete(ClusteredDeleteCommand command,
                          List<SecondaryUndoMutation> secondaryMutations) {
        // 1. 事务行锁和旧记录物化先于业务 MTR；miss 分支不创建 undo 或 dirty page。
        if (command == null) {
            throw new DatabaseValidationException("clustered delete command must not be null");
        }
        if (secondaryMutations == null) {
            throw new DatabaseValidationException("clustered delete secondary mutations must not be null");
        }
        requireOpenForDml();
        Transaction txn = command.transaction();
        TransactionId txnId = transactionManager.assignWriteId(txn);
        BTreeCurrentReadRequest request = currentReadRequest(txn, txnId, command.lockWaitTimeout());
        Optional<BTreeLookupResult> locked = currentRead.lockPoint(command.index(), command.key(),
                request, BTreeCurrentReadMode.FOR_UPDATE);
        if (locked.isEmpty()) {
            return new DmlWriteResult(false, 0, redo.currentLsn(), txnId);
        }
        // 2. 删除前隐藏列/全量旧 image/secondary tail 共同定义 rollback revive 所需的完整证据。
        BTreeLookupResult old = locked.orElseThrow();
        HiddenColumns oldHidden = requireHiddenColumns(old.record(), "delete");

        List<LobVersionOwnership> lobOwnerships = planDeleteLobOwnerships(command, old.record());
        preflightLobSegment(command.lobSegment(), !lobOwnerships.isEmpty());
        UndoWritePlan undoPlan = undoLogManager.planDelete(txn, command.tableId(), command.index().indexId(),
                command.key().values(), old.record().columnValues(), oldHidden,
                lobOwnerships, secondaryMutations, command.index().keyDef(), command.index().schema());
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.CLUSTERED_DELETE,
                DmlRedoBudgetEstimator.pointRewrite(command.index(), undoPlan)));
        boolean undoWritten = false;
        try {
            // 3. 先固化 undo 并取得 roll pointer，再以旧隐藏列作 CAS 证据发布聚簇 delete mark。
            RollPointer rollPointer = undoLogManager.appendPlanned(txn, mtr, undoPlan);
            undoWritten = true;
            BTreeDeleteMarkResult marked = btree.setClusteredDeleteMark(mtr, command.index(), command.key(),
                    true, new HiddenColumns(txnId, rollPointer), oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            // 4. commit 封闭 undo + 聚簇页物理原子组；本阶段不会释放 leaf page 给 FSP。
            Lsn endLsn = mtrManager.commit(mtr);
            return new DmlWriteResult(marked.changed(), marked.changed() ? 1 : 0, endLsn, txnId);
        } catch (RuntimeException e) {
            // 4. 异常先终止 ACTIVE MTR；undo 已写入后的失败不能降级为普通 statement 可重试错误。
            rollbackActiveMtr(mtr, e);
            if (undoWritten && !(e instanceof UndoWriteFatalException)) {
                throw new UndoWriteFatalException("clustered delete failed after undo physical write", e);
            }
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("clustered delete failed", e);
        }
    }

    /**
     * 提交数据库事务。顺序为 prepareCommit(只预留提交号) -> undo onCommit 持久化提交状态 ->
     * transaction commit(移出 active/进入 COMMITTED) -> redo durability policy 等待 -> row-lock release。
     * 这样 onCommit 失败时事务仍保持 ACTIVE 且 row locks 不释放，避免恢复期把“已对外提交但 undo 仍 ACTIVE”
     * 的事务误回滚。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param command 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code commit} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DmlOperationException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    public DmlCommitResult commit(DmlCommitCommand command) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (command == null) {
            throw new DatabaseValidationException("DML commit command must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        requireOpenForDml();
        Transaction txn = command.transaction();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        TransactionId txnId = txn.transactionId();
        boolean transactionCommitted = false;
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            transactionManager.prepareCommit(txn);
            undoLogManager.onCommit(txn);
            transactionManager.commit(txn);
            transactionCommitted = true;
            Lsn commitLsn = redo.currentLsn();
            boolean durable = command.durabilityPolicy()
                    .awaitCommitDurable(redo, commitLsn, command.durabilityTimeout());
            if (!durable) {
                throw new DmlOperationException("commit redo did not reach durability policy before timeout");
            }
            int released = releaseLocks(txnId);
            return new DmlCommitResult(txn.transactionNo(), true, released);
        } catch (RuntimeException e) {
            if (transactionCommitted) {
                releaseLocksOnFailure(txnId, e);
            }
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("DML commit failed", e);
        }
    }

    /**
     * 回滚数据库事务。调用 rollback service 沿 undo 链反向应用记录级撤销，只有事务真正进入
     * {@link TransactionState#ROLLED_BACK} 后才释放 row locks。若 preflight/apply 失败而事务仍为 ACTIVE 或
     * ROLLING_BACK，必须保留锁隔离半回滚数据，供同连接重试或重启恢复；提前 releaseAll 会让其它事务改写尚待撤销的版本。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param command 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code rollback} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DmlOperationException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    public DmlRollbackResult rollback(DmlRollbackCommand command) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (command == null) {
            throw new DatabaseValidationException("DML rollback command must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        requireOpenForDml();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        Transaction txn = command.transaction();
        TransactionId txnId = txn.transactionId();
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            RollbackSummary summary = rollbackService.rollback(txn, command.clusteredIndex());
            int released = releaseLocks(txnId);
            return new DmlRollbackResult(summary, released);
        } catch (RuntimeException e) {
            if (txn.state() == TransactionState.ROLLED_BACK) {
                // rollback 已完成、仅锁清理自身失败时可重试 release；非终态必须继续持锁。
                releaseLocksOnFailure(txnId, e);
            }
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("DML rollback failed", e);
        }
    }

    /**
     * 通过 undo identity/DD resolver 完整回滚事务。该入口支持同一事务跨多表写入，不接收也不猜测最后一个索引；
     * 终态后才释放 row locks，失败时保持 ROLLING_BACK/锁所有权供重试。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param command 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code rollback} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DmlOperationException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    public DmlRollbackResult rollback(ResolvedDmlRollbackCommand command) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (command == null) {
            throw new DatabaseValidationException("resolved DML rollback command must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        requireOpenForDml();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        Transaction txn = command.transaction();
        TransactionId txnId = txn.transactionId();
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            RollbackSummary summary = rollbackService.rollback(txn);
            int released = releaseLocks(txnId);
            return new DmlRollbackResult(summary, released);
        } catch (RuntimeException error) {
            if (txn.state() == TransactionState.ROLLED_BACK) {
                releaseLocksOnFailure(txnId, error);
            }
            if (error instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("resolved DML rollback failed", error);
        }
    }

    private void requireOpenForDml() {
        RecoveryState state = recoveryGate.state();
        if (state != RecoveryState.OPEN) {
            throw new DmlOperationException("DML rejected while recovery gate is " + state);
        }
    }

    private static BTreeCurrentReadRequest currentReadRequest(Transaction txn, TransactionId txnId,
                                                             java.time.Duration lockWaitTimeout) {
        return new BTreeCurrentReadRequest(txnId, txn.isolationLevel(), lockWaitTimeout,
                DEFAULT_RELOCATION_RETRIES);
    }

    /**
     * 校验当前状态后推进存储引擎稳定 API状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param original 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private void rollbackActiveMtr(MiniTransaction mtr, RuntimeException original) {
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            return;
        }
        try {
            mtrManager.rollbackUncommitted(mtr);
        } catch (RuntimeException rollbackError) {
            original.addSuppressed(rollbackError);
        }
    }

    /**
     * 在 begin 前把超过 256B 的 LOB raw value 替换为定长 placeholder，并冻结每列写计划/undo ownership。
     * inline、NULL 和普通类型原样保留；只有确实需要 externalization 时才要求命令携带权威 LOB segment。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param command 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code planInsertLobs} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private PlannedInsertLobs planInsertLobs(ClusteredInsertCommand command) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        List<ColumnValue> source = command.record().columnValues();
        if (source.size() != command.index().schema().columns().size()) {
            throw new DatabaseValidationException("clustered INSERT row width differs from table schema");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        List<ColumnValue> placeholders = new ArrayList<>(source);
        List<PlannedLobValue> values = new ArrayList<>();
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        List<InsertedLobOwnership> ownerships = new ArrayList<>();
        for (int ordinal = 0; ordinal < source.size(); ordinal++) {
            var column = command.index().schema().column(ordinal);
            ColumnValue value = source.get(ordinal);
            if (column.type().storageKind() != StorageKind.OVERFLOW_CAPABLE
                    || value instanceof ColumnValue.NullValue
                    || !lobStorage.requiresExternalization(column.type(), value)) {
                continue;
            }
            var segment = command.lobSegment().orElseThrow(() -> new DmlLobBindingException(
                    "external LOB column requires table LOB segment: " + column.name()));
            LobWritePlan plan = lobStorage.planWrite(segment, column.type(), value);
            LobReference placeholderReference = new LobReference(segment.spaceId(), PageNo.of(4L + ordinal),
                    plan.totalLength(), plan.pageCount(), segment.segmentId(), segment.inodeSlot(), plan.crc32());
            ColumnValue.ExternalValue placeholder = new ColumnValue.ExternalValue(column.type().typeId(),
                    placeholderReference, plan.inlinePrefix());
            placeholders.set(ordinal, placeholder);
            PlannedLobValue planned = new PlannedLobValue(ordinal, plan);
            values.add(planned);
            ownerships.add(new InsertedLobOwnership(ordinal, placeholder));
        }
        LogicalRecord placeholderRecord = new LogicalRecord(command.record().schemaVersion(), placeholders,
                command.record().deleted(), command.record().recordType(), command.record().hiddenColumns());
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return new PlannedInsertLobs(placeholderRecord, values, ownerships);
    }

    /**
     * 对比 current old row 与目标完整行，冻结 UPDATE 的 placeholder record、新 LOB write plans 和版本 ownership。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验新旧行宽度与 exact schema 一致；普通列直接保留目标值。</li>
     *     <li>未变化的 external envelope 原样复用，拒绝调用方注入另一条物理 external reference。</li>
     *     <li>对 raw LOB 判断 inline/external；新 external 生成 placeholder/write plan，旧 external 生成 purge-old 动作。</li>
     *     <li>按 ordinal 发布不可变 placeholder row、write plan 和 LV ownership 聚合。</li>
     * </ol>
     *
     * @param command UPDATE 命令，提供目标完整行和可选 authoritative LOB segment。
     * @param oldRow  FOR_UPDATE current-read 物化的旧完整聚簇记录。
     * @return 可在 undo admission 和业务 MTR 中复用的不可变 UPDATE LOB 计划。
     * @throws DatabaseValidationException 行宽、external 注入或目标列值不满足 schema/LOB 约束时抛出。
     * @throws DmlLobBindingException 需要写/回收 external chain 但命令缺少 LOB segment 时抛出。
     */
    private PlannedUpdateLobs planUpdateLobs(ClusteredUpdateCommand command, LogicalRecord oldRow) {
        // 1. UPDATE old/new image 必须属于同一 exact schema，避免 ownership ordinal 指向不同列。
        List<ColumnValue> oldValues = oldRow.columnValues();
        List<ColumnValue> targetValues = command.newRecord().columnValues();
        if (oldValues.size() != command.index().schema().columnCount()
                || targetValues.size() != oldValues.size()) {
            throw new DatabaseValidationException("clustered UPDATE old/new row width differs from schema");
        }
        List<ColumnValue> placeholders = new ArrayList<>(targetValues);
        List<PlannedLobValue> plannedValues = new ArrayList<>();
        List<LobVersionOwnership> ownerships = new ArrayList<>();
        for (int ordinal = 0; ordinal < targetValues.size(); ordinal++) {
            var column = command.index().schema().column(ordinal);
            if (column.type().storageKind() != StorageKind.OVERFLOW_CAPABLE) continue;
            ColumnValue oldValue = oldValues.get(ordinal);
            ColumnValue targetValue = targetValues.get(ordinal);

            // 2. 只有锁定旧行已经持有的同一 external envelope 可以跨版本复用；其它 physical reference 没有授权来源。
            if (targetValue instanceof ColumnValue.ExternalValue targetExternal) {
                if (!targetExternal.equals(oldValue)) {
                    throw new DatabaseValidationException(
                            "clustered UPDATE cannot inject a new external LOB reference at ordinal " + ordinal);
                }
                continue;
            }

            // 3. raw 值先由 LobCodec 完整校验；超过 inline 阈值时冻结 placeholder 和真实 write plan。
            boolean externalize = !(targetValue instanceof ColumnValue.NullValue)
                    && lobStorage.requiresExternalization(column.type(), targetValue);
            boolean purgeOld = oldValue instanceof ColumnValue.ExternalValue;
            if (externalize) {
                var segment = command.lobSegment().orElseThrow(() -> new DmlLobBindingException(
                        "LOB replacement requires table LOB segment: " + column.name()));
                LobWritePlan plan = lobStorage.planWrite(segment, column.type(), targetValue);
                LobReference placeholderReference = new LobReference(segment.spaceId(), PageNo.of(4L + ordinal),
                        plan.totalLength(), plan.pageCount(), segment.segmentId(), segment.inodeSlot(), plan.crc32());
                ColumnValue.ExternalValue placeholder = new ColumnValue.ExternalValue(column.type().typeId(),
                        placeholderReference, plan.inlinePrefix());
                placeholders.set(ordinal, placeholder);
                plannedValues.add(new PlannedLobValue(ordinal, plan));
                ownerships.add(new LobVersionOwnership(ordinal, purgeOld, Optional.of(placeholder)));
            } else if (purgeOld) {
                ownerships.add(new LobVersionOwnership(ordinal, true, Optional.empty()));
            }
        }
        // 4. schema 顺序循环天然保证 ownership ordinal 递增；构造器防御性冻结所有集合。
        LogicalRecord placeholderRecord = new LogicalRecord(command.newRecord().schemaVersion(), placeholders,
                false, command.newRecord().recordType(), null);
        return new PlannedUpdateLobs(placeholderRecord, plannedValues, ownerships);
    }

    /**
     * 从 DELETE 的锁定旧行提取全部 external old-chain purge ownership。
     *
     * @param command DELETE 命令；旧行含 external value 时必须携带 authoritative LOB segment。
     * @param oldRow  删除前存活版本的完整聚簇记录。
     * @return 按 schema ordinal 递增、只含 purge-old 动作的不可变 ownership 列表。
     * @throws DmlLobBindingException 旧行含 external value但命令没有 LOB segment 时抛出。
     */
    private List<LobVersionOwnership> planDeleteLobOwnerships(ClusteredDeleteCommand command,
                                                               LogicalRecord oldRow) {
        List<LobVersionOwnership> ownerships = new ArrayList<>();
        for (int ordinal = 0; ordinal < oldRow.columnValues().size(); ordinal++) {
            if (oldRow.columnValues().get(ordinal) instanceof ColumnValue.ExternalValue) {
                int columnOrdinal = ordinal;
                command.lobSegment().orElseThrow(() -> new DmlLobBindingException(
                        "DELETE old external LOB requires table LOB segment at ordinal " + columnOrdinal));
                ownerships.add(new LobVersionOwnership(ordinal, true, Optional.empty()));
            }
        }
        return List.copyOf(ownerships);
    }

    /**
     * 用独立短读 MTR 在业务 undo/B+Tree 写之前验证 authoritative LOB segment identity 和 purpose。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param segment  exact DD binding 的可选 segment 容器。
     * @param required 本次 INSERT/UPDATE/DELETE 是否会写新链或持久化旧链 purge ownership。
     * @throws DmlLobBindingException required 但 binding 缺失，或 segment purpose/identity 预检失败时抛出。
     * @throws DatabaseRuntimeException 只读 MTR、FSP metadata 或资源释放失败时抛出并保留 cause。
     */
    private void preflightLobSegment(Optional<cn.zhangyis.db.storage.api.SegmentRef> segment,
                                     boolean required) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        if (!required) {
            return;
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        var authoritative = segment.orElseThrow(() -> new DmlLobBindingException(
                "LOB operation requires authoritative table LOB segment"));
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        MiniTransaction check = mtrManager.beginReadOnly();
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        try {
            // 每张表只有一个 LOB segment；多列计划复用同一 identity，复核一次即可。
            lobStorage.preflightSegment(check, authoritative);
            mtrManager.commit(check);
        } catch (RuntimeException error) {
            rollbackActiveMtr(check, error);
            if (error instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlLobBindingException("LOB segment preflight failed", error);
        }
    }

    /**
     * 失败收尾顺序：先反序补偿尚未转移的 LOB allocation，再关闭 index prepare scope，最后关闭 undo prepare。
     * 每个 close 失败作为 suppressed 保留；上层随后 rollback MTR memo，但不会误称页内容已撤销。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param allocations   已写出但尚未全部转移 ownership 的 INSERT LOB guards。
     * @param preparedInsert 已固定聚簇插入路径/slot 的 guard；可为 {@code null}。
     * @param preparedUndo  已固定 INSERT undo owner/root 的 guard；可为 {@code null}。
     * @param primary       触发收尾的原始异常。
     * @return 附带全部 cleanup suppressed 异常的原始失败对象。
     */
    private static RuntimeException compensateAndClosePrepared(List<LobWriteAllocation> allocations,
                                                               PreparedClusteredInsert preparedInsert,
                                                               PreparedUndoAppend<InsertedLobOwnership> preparedUndo,
                                                               RuntimeException primary) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        for (int i = allocations.size() - 1; i >= 0; i--) {
            try {
                allocations.get(i).close();
            } catch (RuntimeException cleanup) {
                primary.addSuppressed(cleanup);
            }
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (preparedInsert != null) {
            try {
                preparedInsert.close();
            } catch (RuntimeException cleanup) {
                primary.addSuppressed(cleanup);
            }
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        if (preparedUndo != null) {
            try {
                preparedUndo.close();
            } catch (RuntimeException cleanup) {
                primary.addSuppressed(cleanup);
            }
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return primary;
    }

    /**
     * LOB-aware UPDATE 失败时按 ownership 获取逆序补偿；每个 cleanup 异常都压入主失败，不能覆盖原始根因。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param allocations    已写出但尚未全部转移 ownership 的新 LOB guards。
     * @param preparedUpdate 已固定目标 leaf 的聚簇 UPDATE guard；可为 {@code null}。
     * @param preparedUndo   已固定 undo owner/root 的 deferred UPDATE guard；可为 {@code null}。
     * @param primary        触发收尾的原始异常。
     * @return 附带全部 cleanup suppressed 异常的原始失败对象。
     */
    private static RuntimeException compensateAndClosePreparedUpdate(
            List<LobWriteAllocation> allocations,
            PreparedClusteredUpdate preparedUpdate,
            PreparedUndoAppend<LobVersionOwnership> preparedUndo,
            RuntimeException primary) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        for (int i = allocations.size() - 1; i >= 0; i--) {
            try { allocations.get(i).close(); }
            catch (RuntimeException cleanup) { primary.addSuppressed(cleanup); }
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (preparedUpdate != null) {
            try { preparedUpdate.close(); }
            catch (RuntimeException cleanup) { primary.addSuppressed(cleanup); }
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        if (preparedUndo != null) {
            try { preparedUndo.close(); }
            catch (RuntimeException cleanup) { primary.addSuppressed(cleanup); }
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return primary;
    }

    /**
     * 单列需要 externalization 的冻结计划；同时用于 INSERT 和 UPDATE。
     *
     * @param columnOrdinal exact-version schema ordinal；决定 placeholder/actual 替换位置。
     * @param plan          已冻结 payload、segment、页数、CRC、prefix 和 redo workload 的写计划。
     */
    private record PlannedLobValue(int columnOrdinal, LobWritePlan plan) {
        private PlannedLobValue {
            if (columnOrdinal < 0 || plan == null) {
                throw new DatabaseValidationException("invalid planned LOB value");
            }
        }
    }

    /**
     * 一行 INSERT 的 placeholder、逐列 LOB 计划和 placeholder undo ownership。
     *
     * @param placeholderRecord     所有新 external 列已替换为等长 placeholder 的完整用户行。
     * @param values                按 ordinal 递增的新链写计划。
     * @param placeholderOwnerships 与 values 一一对应、进入 deferred INSERT undo 的 ownership。
     */
    private record PlannedInsertLobs(LogicalRecord placeholderRecord, List<PlannedLobValue> values,
                                     List<InsertedLobOwnership> placeholderOwnerships) {
        /**
         * 创建 {@code PlannedInsertLobs}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
         *
         * @param placeholderRecord 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
         * @param values 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
         * @param placeholderOwnerships 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
         * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        private PlannedInsertLobs {
            if (placeholderRecord == null || values == null || placeholderOwnerships == null
                    || values.size() != placeholderOwnerships.size()) {
                throw new DatabaseValidationException("invalid planned INSERT LOB aggregate");
            }
            values = List.copyOf(values);
            placeholderOwnerships = List.copyOf(placeholderOwnerships);
        }

        private List<LobWritePlan> writePlans() {
            return values.stream().map(PlannedLobValue::plan).toList();
        }
    }

    /**
     * 一行 UPDATE 的 placeholder、逐列新链计划和完整 LV ownership。
     *
     * @param placeholderRecord     新 external 列使用定长 placeholder、未变化 external 原样复用的目标行。
     * @param values                本次前向 UPDATE 真正需要创建的新链计划。
     * @param placeholderOwnerships rollback-new placeholder 与 purge-old 动作的完整 LV 列表。
     */
    private record PlannedUpdateLobs(LogicalRecord placeholderRecord, List<PlannedLobValue> values,
                                     List<LobVersionOwnership> placeholderOwnerships) {
        /**
         * 创建 {@code PlannedUpdateLobs}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
         *
         * @param placeholderRecord 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
         * @param values 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
         * @param placeholderOwnerships 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
         * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        private PlannedUpdateLobs {
            if (placeholderRecord == null || values == null || placeholderOwnerships == null) {
                throw new DatabaseValidationException("invalid planned UPDATE LOB aggregate");
            }
            values = List.copyOf(values);
            placeholderOwnerships = List.copyOf(placeholderOwnerships);
        }

        private boolean requiresLobSegment() {
            return !values.isEmpty() || !placeholderOwnerships.isEmpty();
        }

        private List<LobWritePlan> writePlans() {
            return values.stream().map(PlannedLobValue::plan).toList();
        }
    }

    private static HiddenColumns requireHiddenColumns(LogicalRecord record, String operation) {
        HiddenColumns hidden = record.hiddenColumns();
        if (hidden == null) {
            throw new DmlOperationException("clustered " + operation + " requires hidden columns on current row");
        }
        return hidden;
    }

    private static LogicalRecord stampedRecord(LogicalRecord source, boolean deleted, HiddenColumns hiddenColumns) {
        return new LogicalRecord(source.schemaVersion(), source.columnValues(), deleted, source.recordType(),
                hiddenColumns);
    }

    private int releaseLocks(TransactionId txnId) {
        return txnId.isNone() ? 0 : lockManager.releaseAll(txnId);
    }

    /**
     * 按存储引擎稳定 API并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param txnId 参与 {@code releaseLocksOnFailure} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param original 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private void releaseLocksOnFailure(TransactionId txnId, RuntimeException original) {
        if (txnId.isNone()) {
            return;
        }
        try {
            lockManager.releaseAll(txnId);
        } catch (RuntimeException releaseError) {
            original.addSuppressed(releaseError);
        }
    }
}
