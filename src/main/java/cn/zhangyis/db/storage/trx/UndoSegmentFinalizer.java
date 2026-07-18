package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderSnapshot;
import cn.zhangyis.db.storage.undo.RollbackSegmentHistoryBase;
import cn.zhangyis.db.storage.undo.RollbackSegmentFreeListBase;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.FreeUndoSegmentRef;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogState;
import cn.zhangyis.db.storage.undo.UndoPrepareTarget;
import cn.zhangyis.db.storage.undo.UndoHistoryNodeSnapshot;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * undo segment 终态协调器。它按 eligibility 把 FSP segment drop 或 page3 active→cache/free owner 转移放入同一
 * MTR/redo batch，并在 commit 后才发布内存 slot/reuse directory，闭合 INSERT commit、live/recovery rollback 与
 * committed purge 的 crash 边界。
 *
 * <p><b>锁序</b>：预检 MTR 先读 page3、再读普通 undo first page；返回前释放二者。随后独立只读 MTR 读取
 * inode page2 并物化 drop 规模，提交后才进入 finalization 写 MTR。写 MTR 先由
 * {@link UndoSpaceAllocator#dropUndoSegment} 修改 FSP page0/page2，再获取 page3 X latch 做 owner CAS。当前 XDES
 * 内嵌 page0，故不需要逆序例外；任何时刻都不跨 MTR 保留 page latch/fix，也不在内存 slot/history 锁内等待 IO。
 *
 * <p><b>失败语义</b>：可预测的 owner/state/head 冲突全部在物理写前抛出。进入 finalization MTR 后的异常属于
 * fail-stop：MTR 无 content undo，不能承诺同进程 buffer 可重试，故统一抛 {@link UndoFinalizationException}。
 */
public final class UndoSegmentFinalizer {

    /** finalization 预检与最终物理批次的短 MTR 来源。 */
    private final MiniTransactionManager mtrManager;
    /** 打开 undo first page、校验状态并取得 segment handle 的稳定入口。 */
    private final UndoLogSegmentAccess undoAccess;
    /** 在最终 MTR 内释放 segment inode、extent 与 page 的 FSP 端口。 */
    private final UndoSpaceAllocator undoAllocator;
    /** page3 恢复权威仓储；finalization 只允许 expected-owner CAS clear。 */
    private final RollbackSegmentHeaderRepository headerRepository;
    /** page3 提交成功后发布释放的运行期 slot 投影。 */
    private final RollbackSegmentSlotManager slotManager;
    /** cache/free owner 的统一运行期投影与 transition lease 来源。 */
    private final UndoSegmentReuseDirectory reuseDirectory;
    /** 仅测试使用的 commit 后 crash point；生产构造固定 no-op。 */
    private final UndoFinalizationFaultInjector faultInjector;

    /**
     * 构造生产 finalizer；fault injector 固定为 no-op。
     *
     * @param mtrManager       预检与最终写批次的 MTR 来源。
     * @param undoAccess       undo first page 读取入口。
     * @param undoAllocator    segment drop 端口。
     * @param headerRepository page3 owner CAS 仓储。
     * @param slotManager      运行期 slot 投影。
     * @param reuseDirectory 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     */
    public UndoSegmentFinalizer(MiniTransactionManager mtrManager, UndoLogSegmentAccess undoAccess,
                                 UndoSpaceAllocator undoAllocator,
                                 RollbackSegmentHeaderRepository headerRepository,
                                 RollbackSegmentSlotManager slotManager,
                                 UndoSegmentReuseDirectory reuseDirectory) {
        this(mtrManager, undoAccess, undoAllocator, headerRepository, slotManager, reuseDirectory,
                UndoFinalizationFaultInjector.none());
    }

    /** 包内测试构造器，只允许在成功 commit 后注入模拟 crash。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param mtrManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param undoAccess 由组合根提供的 {@code UndoLogSegmentAccess} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param undoAllocator 由组合根提供的 {@code UndoSpaceAllocator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param headerRepository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param slotManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param reuseDirectory 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param faultInjector 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoSegmentFinalizer(MiniTransactionManager mtrManager, UndoLogSegmentAccess undoAccess,
                          UndoSpaceAllocator undoAllocator,
                          RollbackSegmentHeaderRepository headerRepository,
                          RollbackSegmentSlotManager slotManager,
                          UndoSegmentReuseDirectory reuseDirectory,
                          UndoFinalizationFaultInjector faultInjector) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (mtrManager == null || undoAccess == null || undoAllocator == null || headerRepository == null
                || slotManager == null || reuseDirectory == null || faultInjector == null) {
            throw new DatabaseValidationException("undo finalizer collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.undoAccess = undoAccess;
        this.undoAllocator = undoAllocator;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.headerRepository = headerRepository;
        this.slotManager = slotManager;
        this.reuseDirectory = reuseDirectory;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.faultInjector = faultInjector;
    }

    /**
     * 原子持久化 XA phase one；本方法只处理物理 first-page 与 redo，不发布 live Transaction 状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 ACTIVE 写事务、NONE commit no与一或两条普通 undo binding。</li>
     *     <li>逐 binding 短读核对内存 slot、page3 owner和 ACTIVE first-page，返回前释放全部 latch。</li>
     *     <li>一个写 MTR 按 PageId 固定全部 first page，统一写 PREPARED并追加 ACTIVE→PREPARED redo。</li>
     *     <li>提交并返回 phase-one end LSN；失败保持内存事务 ACTIVE，调用方按 fail-stop 关闭实例。</li>
     * </ol>
     *
     * @param transaction 当前仍为 ACTIVE、已分配 write id且至少写过一条 ordinary undo 的事务
     * @param context transaction 唯一拥有的 undo context
     * @return 同时覆盖全部 PREPARED first-page after-image与 phase-one delta 的 MTR end LSN
     * @throws TransactionStateException 状态、身份、提交号或 undo context 不满足 phase-one 条件时抛出
     * @throws UndoFinalizationException 最终物理 MTR 修改或提交失败时抛出；调用方不得继续普通 DML
     */
    Lsn prepareTransaction(Transaction transaction, UndoContext context) {
        // 1、v1 只接受已经产生普通 undo 的真实写分支。
        if (transaction == null || context == null
                || transaction.state() != TransactionState.ACTIVE
                || transaction.transactionId().isNone()
                || !transaction.transactionNo().isNone()
                || transaction.undoContext() != context
                || context.bindings().isEmpty()) {
            throw new TransactionStateException(
                    "undo prepare requires ACTIVE write transaction with ordinary undo and no commit number");
        }
        List<UndoLogBinding> ordered = context.bindings().stream()
                .sorted(Comparator.comparingInt((UndoLogBinding binding) ->
                                binding.firstPageId().spaceId().value())
                        .thenComparingLong(binding -> binding.firstPageId().pageNo().value()))
                .toList();

        // 2、慢速 page3/first-page 预检分散到短只读 MTR，不把 latch带进 redo admission。
        for (UndoLogBinding binding : ordered) {
            prepareActive(binding, transaction.transactionId(), false);
        }

        // 3、first-page-only 批次先全量复核再写 state，防止 mixed transaction 半 prepared。
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.UNDO_FINALIZATION, UndoRedoBudgetEstimator.prepare(ordered.size())));
        try {
            undoAccess.markPreparedFirstPages(mtr, transaction.transactionId(),
                    ordered.stream().map(binding ->
                            new UndoPrepareTarget(binding.firstPageId(), binding.kind())).toList());
            TransactionStateRedoDeltas.appendPrepare(mtr, transaction);
            // 4、返回值是 phase-one durable wait 的精确目标；内存状态由上层在本 commit 返回后发布。
            return mtrManager.commit(mtr);
        } catch (RuntimeException error) {
            rollbackActiveMtr(mtr, error);
            throw new UndoFinalizationException("XA prepare first-page batch failed", error);
        }
    }

    /**
     * 原子提交事务拥有的独立 undo logs。INSERT-only 按 eligibility cache/free/drop；UPDATE-only 只写 COMMITTED
     * header；mixed 在同一 MTR 中先终结 INSERT owner，再写 UPDATE header，最后只追加一次事务 commit delta。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param context 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param historyLease 参与 {@code finalizeCommit} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     * @throws UndoFinalizationException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    void finalizeCommit(Transaction transaction, UndoContext context, HistoryList.AppendLease historyLease) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (transaction == null || context == null || transaction.state() != TransactionState.ACTIVE
                || transaction.transactionId().isNone() || transaction.transactionNo().isNone()) {
            throw new TransactionStateException("undo commit finalization requires ACTIVE write transaction and commitNo");
        }
        UndoLogBinding insert = context.binding(UndoLogKind.INSERT);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        UndoLogBinding update = context.binding(UndoLogKind.UPDATE);
        if (insert == null && update == null) {
            throw new TransactionStateException("undo commit finalization requires at least one undo log");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if ((update == null) != (historyLease == null)) {
            throw new TransactionStateException("UPDATE undo commit requires exactly one history append lease");
        }
        PreparedHistoryAppend historyAppend = update == null ? null
                : prepareHistoryAppend(update, transaction, context.affectedTableIds(),
                historyLease, UndoLogState.ACTIVE);
        if (insert == null) {
            commitUpdateOnly(transaction, historyAppend, historyLease);
            return;
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try (RollbackSegmentSlotManager.BatchFinalizationLease lease =
                     slotManager.beginBatchFinalization(List.of(insert))) {
            PreparedActive insertPrepared = prepareActive(insert, transaction.transactionId(), false);
            UndoSegmentDropPlan insertDrop = inspectDropPlan(insertPrepared.handle());
            try (ReusePushGroup reusePushes = reserveReusePushes(
                    List.of(insertPrepared), List.of(insertDrop))) {
                FinalizationDisposition insertDisposition = reusePushes.dispositions().getFirst();
                boolean cached = insertDisposition.cachePush() != null;
                boolean free = insertDisposition.free();
                MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                        RedoBudgetPurpose.UNDO_COMMIT,
                        UndoRedoBudgetEstimator.commit(insertDrop, cached, free)));
                try {
                    if (historyLease != null) {
                        historyLease.physicalMutationStarted();
                    }
                    lease.physicalMutationStarted();
                    reusePushes.physicalMutationStarted();
                    if (!insertDisposition.retained()) {
                        undoAllocator.dropUndoSegment(mtr, insertPrepared.handle());
                    }
                    publishFinalizationOwners(mtr, List.of(insertDisposition));
                    List<CachedUndoSegmentRef> cacheResets = cached
                            ? List.of(new CachedUndoSegmentRef(UndoLogKind.INSERT, insertPrepared.handle()))
                            : List.of();
                    List<FreeUndoSegmentRef> freeResets = free
                            ? List.of(new FreeUndoSegmentRef(insertPrepared.handle())) : List.of();
                    if (update != null) {
                        headerRepository.appendHistory(mtr, update.firstPageId().spaceId(),
                                historyAppend.base(), update.slotId(), update.firstPageId(),
                                transaction.transactionNo());
                        undoAccess.appendHistoryNode(mtr,
                                historyAppend.oldTail().map(UndoHistoryNodeSnapshot::firstPageId),
                                update.firstPageId(), transaction.transactionId(), transaction.transactionNo(),
                                cacheResets, freeResets, reusePushes.oldFreeTail());
                    } else if (insertDisposition.retained()) {
                        undoAccess.finalizeActiveReusablePages(mtr, transaction.transactionId(),
                                cacheResets, freeResets, reusePushes.oldFreeTail());
                    }
                    TransactionStateRedoDeltas.appendCommit(mtr, transaction);
                    mtrManager.commit(mtr);
                } catch (RuntimeException error) {
                    rollbackActiveMtr(mtr, error);
                    throw new UndoFinalizationException("atomic undo commit finalization failed", error);
                }
                UndoLogBinding diagnostic = update == null ? insert : update;
                faultInjector.afterCommit(update == null ? UndoFinalizationKind.INSERT_COMMIT
                                : UndoFinalizationKind.UPDATE_COMMIT,
                        diagnostic.slotId(), diagnostic.firstPageId());
                try {
                    reusePushes.complete();
                    lease.complete();
                    if (historyLease != null) {
                        historyLease.complete();
                    }
                } catch (RuntimeException error) {
                    throw new UndoFinalizationException(
                            "undo commit finalization persisted but memory publication failed", error);
                }
            }
        }
    }

    /**
     * 校验当前状态后推进事务、MVCC 与锁状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param prepared 当前算法已准备的中间状态；不得为 {@code null}，必须由本次扫描、日志组装或事务终结流程创建且尚未发布
     * @param historyLease 参与 {@code commitUpdateOnly} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws UndoFinalizationException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private void commitUpdateOnly(Transaction transaction, PreparedHistoryAppend prepared,
                                  HistoryList.AppendLease historyLease) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.UNDO_COMMIT, UndoRedoBudgetEstimator.commit(null)));
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        try {
            historyLease.physicalMutationStarted();
            UndoLogBinding update = prepared.update().binding();
            headerRepository.appendHistory(mtr, update.firstPageId().spaceId(), prepared.base(),
                    update.slotId(), update.firstPageId(), transaction.transactionNo());
            undoAccess.appendHistoryNode(mtr,
                    prepared.oldTail().map(UndoHistoryNodeSnapshot::firstPageId), update.firstPageId(),
                    transaction.transactionId(), transaction.transactionNo());
            TransactionStateRedoDeltas.appendCommit(mtr, transaction);
            mtrManager.commit(mtr);
        } catch (RuntimeException error) {
            rollbackActiveMtr(mtr, error);
            throw new UndoFinalizationException("UPDATE undo commit finalization failed", error);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoLogBinding update = prepared.update().binding();
        faultInjector.afterCommit(UndoFinalizationKind.UPDATE_COMMIT, update.slotId(), update.firstPageId());
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            historyLease.complete();
        } catch (RuntimeException error) {
            throw new UndoFinalizationException(
                    "UPDATE undo commit persisted but history publication failed", error);
        }
    }

    /**
     * 原子提交 PREPARED 事务的 undo owner。prepared INSERT segment 一律 drop；UPDATE segment 直接从
     * PREPARED 转为 COMMITTED并挂 history，避免普通 ACTIVE cache/free 路径放宽物理前置状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 PREPARED、提交号、undo binding与 history lease/affected-table identity。</li>
     *     <li>分别短读核对 UPDATE history append和可选 INSERT page3/PREPARED owner，再读取 INSERT drop plan。</li>
     *     <li>最终 MTR 先 drop INSERT FSP segment并 clear slot，再挂 UPDATE history，最后追加 prepared commit redo。</li>
     *     <li>MTR commit 后发布 slot/history内存投影；失败时事务保持 PREPARED且 row locks不释放。</li>
     * </ol>
     *
     * @param transaction 已由 phase two 预留提交号的 PREPARED 事务
     * @param context transaction 持有的一或两条普通 undo binding
     * @param affectedTableIds 当前 UPDATE logical chain 的去重稳定表集合
     * @param historyLease UPDATE 存在时必须提供的 history append lease；INSERT-only 必须为 null
     * @throws TransactionStateException 状态、提交号、binding或 lease组合非法时抛出
     * @throws UndoFinalizationException 物理 phase-two批次或内存 owner发布失败时抛出
     */
    void finalizePreparedCommit(Transaction transaction, UndoContext context,
                                java.util.Set<Long> affectedTableIds,
                                HistoryList.AppendLease historyLease) {
        // 1、phase-two commit 不能复用普通 ACTIVE finalizer。
        if (transaction == null || context == null || affectedTableIds == null
                || transaction.state() != TransactionState.PREPARED
                || transaction.transactionId().isNone() || transaction.transactionNo().isNone()
                || transaction.undoContext() != context) {
            throw new TransactionStateException(
                    "prepared undo commit requires PREPARED write transaction and commit number");
        }
        UndoLogBinding insert = context.binding(UndoLogKind.INSERT);
        UndoLogBinding update = context.binding(UndoLogKind.UPDATE);
        if (insert == null && update == null) {
            throw new TransactionStateException("prepared commit requires at least one undo log");
        }
        if ((update == null) != (historyLease == null)) {
            throw new TransactionStateException(
                    "prepared UPDATE commit requires exactly one history append lease");
        }

        // 2、所有读预检都在最终写 MTR 前完成，不跨 FSP drop/history append持有 first-page latch。
        PreparedHistoryAppend historyAppend = update == null ? null
                : prepareHistoryAppend(update, transaction, affectedTableIds,
                historyLease, UndoLogState.PREPARED);
        if (insert == null) {
            commitPreparedUpdateOnly(transaction, historyAppend, historyLease);
            return;
        }
        try (RollbackSegmentSlotManager.BatchFinalizationLease slotLease =
                     slotManager.beginBatchFinalization(List.of(insert))) {
            PreparedActive preparedInsert = prepareOwned(
                    insert, transaction.transactionId(), false, UndoLogState.PREPARED);
            UndoSegmentDropPlan insertDrop = inspectDropPlan(preparedInsert.handle());

            // 3、prepared INSERT 不进入 reuse directory；drop+slot clear与可选 UPDATE history同批。
            MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                    RedoBudgetPurpose.UNDO_COMMIT, UndoRedoBudgetEstimator.commit(insertDrop)));
            try {
                if (historyLease != null) {
                    historyLease.physicalMutationStarted();
                }
                slotLease.physicalMutationStarted();
                undoAllocator.dropUndoSegment(mtr, preparedInsert.handle());
                publishFinalizationOwners(mtr, List.of(
                        new FinalizationDisposition(preparedInsert, insertDrop, null, false)));
                if (historyAppend != null) {
                    UndoLogBinding updateBinding = historyAppend.update().binding();
                    headerRepository.appendHistory(mtr, updateBinding.firstPageId().spaceId(),
                            historyAppend.base(), updateBinding.slotId(), updateBinding.firstPageId(),
                            transaction.transactionNo());
                    undoAccess.appendPreparedHistoryNode(mtr,
                            historyAppend.oldTail().map(UndoHistoryNodeSnapshot::firstPageId),
                            updateBinding.firstPageId(), transaction.transactionId(),
                            transaction.transactionNo());
                }
                TransactionStateRedoDeltas.appendPreparedCommit(mtr, transaction);
                mtrManager.commit(mtr);
            } catch (RuntimeException error) {
                rollbackActiveMtr(mtr, error);
                throw new UndoFinalizationException(
                        "prepared mixed/INSERT commit finalization failed", error);
            }

            // 4、持久 owner先完成，内存 slot/history后发布；发布失败由重启重新恢复。
            UndoLogBinding diagnostic = update == null ? insert : update;
            faultInjector.afterCommit(
                    UndoFinalizationKind.PREPARED_COMMIT,
                    diagnostic.slotId(), diagnostic.firstPageId());
            try {
                slotLease.complete();
                if (historyLease != null) {
                    historyLease.complete();
                }
            } catch (RuntimeException error) {
                throw new UndoFinalizationException(
                        "prepared commit persisted but memory publication failed", error);
            }
        }
    }

    /** UPDATE-only prepared commit；没有需要转移的 INSERT slot owner。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param prepared 当前算法已准备的中间状态；不得为 {@code null}，必须由本次扫描、日志组装或事务终结流程创建且尚未发布
     * @param historyLease 参与 {@code commitPreparedUpdateOnly} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws UndoFinalizationException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private void commitPreparedUpdateOnly(Transaction transaction, PreparedHistoryAppend prepared,
                                          HistoryList.AppendLease historyLease) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.UNDO_COMMIT, UndoRedoBudgetEstimator.commit(null)));
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        try {
            historyLease.physicalMutationStarted();
            UndoLogBinding update = prepared.update().binding();
            headerRepository.appendHistory(mtr, update.firstPageId().spaceId(), prepared.base(),
                    update.slotId(), update.firstPageId(), transaction.transactionNo());
            undoAccess.appendPreparedHistoryNode(mtr,
                    prepared.oldTail().map(UndoHistoryNodeSnapshot::firstPageId),
                    update.firstPageId(), transaction.transactionId(), transaction.transactionNo());
            TransactionStateRedoDeltas.appendPreparedCommit(mtr, transaction);
            mtrManager.commit(mtr);
        } catch (RuntimeException error) {
            rollbackActiveMtr(mtr, error);
            throw new UndoFinalizationException(
                    "prepared UPDATE commit finalization failed", error);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoLogBinding update = prepared.update().binding();
        faultInjector.afterCommit(
                UndoFinalizationKind.PREPARED_COMMIT, update.slotId(), update.firstPageId());
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            historyLease.complete();
        } catch (RuntimeException error) {
            throw new UndoFinalizationException(
                    "prepared UPDATE commit persisted but history publication failed", error);
        }
    }

    /**
     * live full rollback：只允许回收 ACTIVE 且持久/内存 logical head 都为 EMPTY 的 segment。
     *
     * @param transaction 正处于 ROLLING_BACK 的 live 事务。
     * @param context     已发布 EMPTY logical head 的 undo context。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    void finalizeLiveRollback(Transaction transaction, UndoContext context) {
        if (transaction == null || context == null) {
            throw new DatabaseValidationException("rollback finalization transaction/context must not be null");
        }
        if (transaction.state() != TransactionState.ROLLING_BACK
                || context.bindings().stream().anyMatch(binding -> !binding.logicalHead().isEmpty())) {
            throw new TransactionStateException("live rollback finalization requires ROLLING_BACK and all EMPTY heads");
        }
        finalizeActiveBatch(UndoFinalizationKind.LIVE_ROLLBACK, context.bindings(),
                transaction.transactionId(), transaction);
    }

    /**
     * prepared rollback 到双 EMPTY 后原子 drop全部 segment并清 page3 owner。prepared owner 不进入 cache/free，
     * 因而不会放宽普通 ACTIVE reusable-page helper 的状态前置条件。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验运行时 PREPARED_ROLLING_BACK 与全部内存 logical head=EMPTY。</li>
     *     <li>取得全部 slot finalization lease，逐 owner短读核对 PREPARED状态并读取 FSP drop plan。</li>
     *     <li>单 MTR drop全部 segment、clear全部 page3 slots并追加 PREPARED→ROLLED_BACK redo。</li>
     *     <li>物理提交后发布内存 slot释放；失败时不允许事务 manager发布终态。</li>
     * </ol>
     *
     * @param transaction 正处于 prepared rollback 重试态的 live/rebuilt事务
     * @param context 已经把一或两条 logical head持久推进到 EMPTY 的 undo context
     * @throws TransactionStateException 状态、owner或 logical head不满足终结条件时抛出
     * @throws UndoFinalizationException drop/page3/redo批次或内存发布失败时抛出
     */
    void finalizePreparedRollback(Transaction transaction, UndoContext context) {
        // 1、prepared rollback 的磁盘 owner仍是 PREPARED，不能交给普通 ACTIVE batch。
        if (transaction == null || context == null
                || transaction.state() != TransactionState.PREPARED_ROLLING_BACK
                || transaction.undoContext() != context
                || context.bindings().isEmpty()
                || context.bindings().stream().anyMatch(
                binding -> !binding.logicalHead().isEmpty())) {
            throw new TransactionStateException(
                    "prepared rollback finalization requires PREPARED_ROLLING_BACK and EMPTY heads");
        }
        List<UndoLogBinding> ordered = context.bindings().stream()
                .sorted(Comparator.comparingInt((UndoLogBinding binding) ->
                                binding.firstPageId().spaceId().value())
                        .thenComparingLong(binding -> binding.firstPageId().pageNo().value()))
                .toList();
        try (RollbackSegmentSlotManager.BatchFinalizationLease lease =
                     slotManager.beginBatchFinalization(ordered)) {
            // 2、只读预检与 FSP plan读取均在最终写 MTR前完成。
            List<PreparedActive> prepared = ordered.stream()
                    .map(binding -> prepareOwned(binding, transaction.transactionId(),
                            true, UndoLogState.PREPARED))
                    .toList();
            List<UndoSegmentDropPlan> plans = prepared.stream()
                    .map(item -> inspectDropPlan(item.handle()))
                    .toList();

            // 3、prepared owner全部 drop；不触碰 reuse directory或 first-page reset。
            MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                    RedoBudgetPurpose.UNDO_FINALIZATION,
                    UndoRedoBudgetEstimator.finalization(plans, true)));
            try {
                lease.physicalMutationStarted();
                for (PreparedActive item : prepared) {
                    undoAllocator.dropUndoSegment(mtr, item.handle());
                }
                List<FinalizationDisposition> dispositions = new ArrayList<>(prepared.size());
                for (int index = 0; index < prepared.size(); index++) {
                    dispositions.add(new FinalizationDisposition(
                            prepared.get(index), plans.get(index), null, false));
                }
                publishFinalizationOwners(mtr, dispositions);
                TransactionStateRedoDeltas.appendPreparedRollback(mtr, transaction);
                mtrManager.commit(mtr);
            } catch (RuntimeException error) {
                rollbackActiveMtr(mtr, error);
                throw new UndoFinalizationException(
                        "prepared rollback finalization failed", error);
            }

            // 4、slot runtime projection 只能落后、不能领先物理 page3 clear。
            UndoLogBinding diagnostic = ordered.getFirst();
            faultInjector.afterCommit(UndoFinalizationKind.PREPARED_ROLLBACK,
                    diagnostic.slotId(), diagnostic.firstPageId());
            try {
                lease.complete();
            } catch (RuntimeException error) {
                throw new UndoFinalizationException(
                        "prepared rollback persisted but slot publication failed", error);
            }
        }
    }

    /** recovery 对同一 creator 的 INSERT/UPDATE slots 做一次原子 cache/free/drop owner 终结与 terminal delta。
     *
     * @param bindings 参与 {@code finalizeRecoveredRollback} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param creatorTransactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     */
    void finalizeRecoveredRollback(Collection<UndoLogBinding> bindings, TransactionId creatorTransactionId) {
        finalizeActiveBatch(UndoFinalizationKind.RECOVERY_ROLLBACK, bindings, creatorTransactionId, null);
    }

    private void finalizeActiveBatch(UndoFinalizationKind kind, Collection<UndoLogBinding> bindings,
                                     TransactionId creator, Transaction transaction) {
        if (bindings == null || bindings.isEmpty()) {
            throw new DatabaseValidationException("undo rollback batch must not be empty");
        }
        List<UndoLogBinding> ordered = bindings.stream()
                .sorted(Comparator.comparingInt((UndoLogBinding binding) -> binding.firstPageId().spaceId().value())
                        .thenComparingLong(binding -> binding.firstPageId().pageNo().value()))
                .toList();
        try (RollbackSegmentSlotManager.BatchFinalizationLease lease =
                     slotManager.beginBatchFinalization(ordered)) {
            List<PreparedActive> prepared = ordered.stream()
                    .map(binding -> prepareActive(binding, creator, true))
                    .toList();
            List<UndoSegmentDropPlan> plans = prepared.stream()
                    .map(item -> inspectDropPlan(item.handle()))
                    .toList();
            try (ReusePushGroup reusePushes = reserveReusePushes(prepared, plans)) {
                List<FinalizationDisposition> dispositions = reusePushes.dispositions();
                List<UndoSegmentDropPlan> droppedPlans = dispositions.stream()
                        .filter(item -> !item.retained())
                        .map(FinalizationDisposition::dropPlan)
                        .toList();
                int cachedCount = (int) dispositions.stream().filter(item -> item.cachePush() != null).count();
                int freeCount = (int) dispositions.stream().filter(FinalizationDisposition::free).count();
                MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                        RedoBudgetPurpose.UNDO_FINALIZATION,
                        UndoRedoBudgetEstimator.finalization(droppedPlans, cachedCount, freeCount, true)));
                try {
                    lease.physicalMutationStarted();
                    reusePushes.physicalMutationStarted();
                    for (FinalizationDisposition disposition : dispositions) {
                        if (!disposition.retained()) {
                            undoAllocator.dropUndoSegment(mtr, disposition.prepared().handle());
                        }
                    }
                    publishFinalizationOwners(mtr, dispositions);
                    List<CachedUndoSegmentRef> cacheResets = dispositions.stream()
                            .filter(item -> item.cachePush() != null)
                            .map(item -> new CachedUndoSegmentRef(item.prepared().binding().kind(),
                                    item.prepared().handle()))
                            .toList();
                    List<FreeUndoSegmentRef> freeResets = dispositions.stream()
                            .filter(FinalizationDisposition::free)
                            .map(item -> new FreeUndoSegmentRef(item.prepared().handle()))
                            .toList();
                    if (!cacheResets.isEmpty() || !freeResets.isEmpty()) {
                        undoAccess.finalizeActiveReusablePages(mtr, creator, cacheResets, freeResets,
                                reusePushes.oldFreeTail());
                    }
                    if (kind == UndoFinalizationKind.LIVE_ROLLBACK) {
                        TransactionStateRedoDeltas.appendRollbackComplete(mtr, transaction);
                    } else {
                        TransactionStateRedoDeltas.appendRecoveredRollback(mtr, creator);
                    }
                    mtrManager.commit(mtr);
                } catch (RuntimeException error) {
                    rollbackActiveMtr(mtr, error);
                    throw new UndoFinalizationException("multi-segment undo rollback finalization failed", error);
                }
                UndoLogBinding diagnostic = ordered.getFirst();
                faultInjector.afterCommit(kind, diagnostic.slotId(), diagnostic.firstPageId());
                try {
                    reusePushes.complete();
                    lease.complete();
                } catch (RuntimeException error) {
                    throw new UndoFinalizationException(
                            "undo rollback finalization persisted but memory publication failed", error);
                }
            }
        }
    }

    private PreparedActive prepareActive(UndoLogBinding binding, TransactionId creator,
                                         boolean requireEmptyHead) {
        return prepareOwned(binding, creator, requireEmptyHead, UndoLogState.ACTIVE);
    }

    /** 核对 ACTIVE 或 PREPARED page3 owner；调用点必须显式传入期望态，禁止宽松二选一。 */
    private PreparedActive prepareOwned(UndoLogBinding binding, TransactionId creator,
                                        boolean requireEmptyHead, UndoLogState expectedState) {
        if (expectedState != UndoLogState.ACTIVE && expectedState != UndoLogState.PREPARED) {
            throw new DatabaseValidationException(
                    "undo owner preflight expected state must be ACTIVE or PREPARED");
        }
        PageId memoryOwner = slotManager.undoFirstPageId(binding.slotId());
        if (!memoryOwner.equals(binding.firstPageId())) {
            throw new UndoLogFormatException("memory rseg slot owner mismatch: expected="
                    + binding.firstPageId() + ", current=" + memoryOwner);
        }
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            RollbackSegmentHeaderSnapshot snapshot = headerRepository.read(read,
                    binding.firstPageId().spaceId(), slotManager.rollbackSegmentId(), slotManager.slotCapacity(),
                    reuseDirectory.capacityPerKind());
            if (!binding.firstPageId().equals(snapshot.occupiedSlots().get(binding.slotId()))) {
                throw new UndoLogFormatException("persistent rseg slot owner mismatch for " + binding.kind());
            }
            UndoLogSegment segment = undoAccess.open(read, binding.firstPageId(), PageLatchMode.SHARED);
            if (!segment.creatorTransactionId().equals(creator) || segment.undoKind() != binding.kind()
                    || expectedState == UndoLogState.ACTIVE && !segment.isActive()
                    || expectedState == UndoLogState.PREPARED && !segment.isPrepared()) {
                throw new UndoLogFormatException(expectedState
                        + " undo binding identity/state mismatch for " + binding.kind());
            }
            if (requireEmptyHead && (!segment.logicalHead().isEmpty() || !binding.logicalHead().isEmpty())) {
                throw new UndoLogFormatException("rollback finalization requires EMPTY " + binding.kind() + " head");
            }
            UndoSegmentHandle handle = segment.handle();
            mtrManager.commit(read);
            return new PreparedActive(binding, handle, snapshot.freeListBase());
        } catch (RuntimeException error) {
            rollbackActiveMtr(read, error);
            throw error;
        }
    }

    /**
     * UPDATE commit 的全量只读预检。page3、new node、old tail 分属独立短 MTR，返回后只保留不可变快照；
     * history transition 已阻止其它 append/unlink 改变运行时链端点，但不持 Java lock 跨 IO。
     */
    private PreparedHistoryAppend prepareHistoryAppend(
            UndoLogBinding update, Transaction transaction, java.util.Set<Long> affectedTableIds,
            HistoryList.AppendLease lease, UndoLogState expectedState) {
        if (update == null || update.kind() != UndoLogKind.UPDATE || transaction == null
                || affectedTableIds == null || lease == null
                || expectedState != UndoLogState.ACTIVE && expectedState != UndoLogState.PREPARED) {
            throw new DatabaseValidationException("history append preparation requires UPDATE binding/lease");
        }
        HistoryEntry expectedEntry = new HistoryEntry(transaction.transactionNo(), transaction.transactionId(),
                update.firstPageId().spaceId(), update.firstPageId(), update.slotId(),
                affectedTableIds);
        if (!expectedEntry.equals(lease.entry())) {
            throw new TransactionStateException("history append lease identity differs from UPDATE binding");
        }
        PageId memoryOwner = slotManager.undoFirstPageId(update.slotId());
        if (!memoryOwner.equals(update.firstPageId())) {
            throw new UndoLogFormatException("memory UPDATE slot owner mismatch before history append");
        }

        RollbackSegmentHeaderSnapshot header;
        MiniTransaction page3Read = mtrManager.beginReadOnly();
        try {
            header = headerRepository.read(page3Read, update.firstPageId().spaceId(),
                    slotManager.rollbackSegmentId(), slotManager.slotCapacity(), reuseDirectory.capacityPerKind());
            mtrManager.commit(page3Read);
        } catch (RuntimeException error) {
            rollbackActiveMtr(page3Read, error);
            throw error;
        }
        if (!update.firstPageId().equals(header.occupiedSlots().get(update.slotId()))) {
            throw new UndoLogFormatException("persistent UPDATE slot owner mismatch before history append");
        }
        requireHistoryBaseMatchesLease(header.historyBase(), lease);

        UndoHistoryNodeSnapshot newNode = inspectHistoryNode(update.firstPageId());
        boolean stateMatches = expectedState == UndoLogState.ACTIVE
                ? newNode.isActive() : newNode.isPrepared();
        if (!stateMatches || newNode.kind() != UndoLogKind.UPDATE
                || !newNode.creatorTransactionId().equals(transaction.transactionId())
                || !newNode.committedTransactionNo().isNone()
                || newNode.previousHistoryPageId().isPresent() || newNode.nextHistoryPageId().isPresent()) {
            throw new UndoLogFormatException(
                    "new history node must be an unlinked " + expectedState + " UPDATE undo first page");
        }
        Optional<UndoHistoryNodeSnapshot> oldTail = lease.expectedTail()
                .map(entry -> inspectHistoryNode(entry.undoFirstPageId()));
        if (oldTail.isPresent()) {
            UndoHistoryNodeSnapshot tail = oldTail.orElseThrow();
            HistoryEntry runtimeTail = lease.expectedTail().orElseThrow();
            if (!tail.isCommitted() || tail.kind() != UndoLogKind.UPDATE
                    || !tail.creatorTransactionId().equals(runtimeTail.creatorTrxId())
                    || !tail.committedTransactionNo().equals(runtimeTail.transactionNo())
                    || tail.nextHistoryPageId().isPresent()) {
                throw new UndoLogFormatException("persistent history tail differs from runtime projection");
            }
        }
        return new PreparedHistoryAppend(new PreparedActive(update, newNode.handle(), header.freeListBase()),
                header.historyBase(), oldTail);
    }

    /** 对运行时冻结的 head/tail/count 与 page3 base 做精确交叉校验。 */
    private static void requireHistoryBaseMatchesLease(RollbackSegmentHistoryBase base,
                                                       HistoryList.TransitionLease lease) {
        Optional<PageId> expectedHead = lease.expectedHead().map(HistoryEntry::undoFirstPageId);
        Optional<PageId> expectedTail = lease.expectedTail().map(HistoryEntry::undoFirstPageId);
        if (base.length() != lease.expectedSize() || !base.headPageId().equals(expectedHead)
                || !base.tailPageId().equals(expectedTail)) {
            throw new UndoLogFormatException("runtime history projection differs from page3 base: base="
                    + base + ", runtimeHead=" + expectedHead + ", runtimeTail=" + expectedTail
                    + ", runtimeSize=" + lease.expectedSize());
        }
    }

    /** 一张 first page 一个只读 MTR，避免沿跨事务 history 链累计 fix/latch。 */
    private UndoHistoryNodeSnapshot inspectHistoryNode(PageId firstPageId) {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            UndoHistoryNodeSnapshot snapshot = undoAccess.inspectHistoryNode(read, firstPageId);
            mtrManager.commit(read);
            return snapshot;
        } catch (RuntimeException error) {
            rollbackActiveMtr(read, error);
            throw error;
        }
    }

    /**
     * committed purge：只允许回收 header identity 与 history entry 完全一致的 COMMITTED segment。
     *
     * @param entry 当前 committed history 队首 identity。
     * @param historyLease 参与 {@code finalizePurgedHistory} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoFinalizationException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    void finalizePurgedHistory(HistoryEntry entry, HistoryList.HeadRemovalLease historyLease) {
        if (entry == null || historyLease == null || !entry.equals(historyLease.expected())) {
            throw new DatabaseValidationException("purge finalization history entry/lease mismatch");
        }
        PreparedPurge prepared = preparePurge(entry, historyLease);
        try (RollbackSegmentSlotManager.FinalizationLease slotLease =
                     slotManager.beginFinalization(entry.slotId(), entry.undoFirstPageId())) {
            UndoSegmentDropPlan dropPlan = inspectDropPlan(prepared.removed().handle());
            PreparedActive active = new PreparedActive(
                    new UndoLogBinding(UndoLogKind.UPDATE, entry.slotId(), entry.undoFirstPageId(),
                            prepared.removed().logicalHead()), prepared.removed().handle(), prepared.freeBase());
            try (ReusePushGroup reusePushes = reserveReusePushes(List.of(active), List.of(dropPlan))) {
                FinalizationDisposition disposition = reusePushes.dispositions().getFirst();
                boolean cached = disposition.cachePush() != null;
                boolean free = disposition.free();
                MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                        RedoBudgetPurpose.UNDO_FINALIZATION,
                        UndoRedoBudgetEstimator.finalization(disposition.retained() ? List.of() : List.of(dropPlan),
                                cached ? 1 : 0, free ? 1 : 0, false)));
                try {
                    historyLease.physicalMutationStarted();
                    slotLease.physicalMutationStarted();
                    reusePushes.physicalMutationStarted();
                    if (!disposition.retained()) {
                        undoAllocator.dropUndoSegment(mtr, prepared.removed().handle());
                    }
                    headerRepository.removeHistoryHead(mtr, entry.undoSpaceId(), prepared.base(),
                            entry.slotId(), entry.undoFirstPageId(),
                            prepared.newHead().map(UndoHistoryNodeSnapshot::firstPageId));
                    publishFinalizationOwners(mtr, List.of(disposition));
                    undoAccess.unlinkHistoryHead(mtr, prepared.removed(), prepared.newHead(), cached,
                            free, reusePushes.oldFreeTail());
                    mtrManager.commit(mtr);
                } catch (RuntimeException error) {
                    rollbackActiveMtr(mtr, error);
                    throw new UndoFinalizationException(
                            "persistent history unlink failed after physical mutation began: " + entry, error);
                }

                faultInjector.afterCommit(UndoFinalizationKind.PURGE, entry.slotId(), entry.undoFirstPageId());
                try {
                    // 新 runtime head 暴露前先完成其旧 owner 的 slot/cache 投影发布。
                    reusePushes.complete();
                    slotLease.complete();
                    historyLease.complete();
                } catch (RuntimeException error) {
                    throw new UndoFinalizationException(
                            "persistent history unlink committed but memory publication failed: " + entry, error);
                }
            }
        }
    }

    /**
     * purge unlink 前只读核对运行时 lease、page3 owner、undo history node 和可选 successor。记录级 purge 必须已经
     * 把 removed node 的 logical head 推进到 EMPTY；否则终结 segment 会让尚未消费的 secondary/LOB ownership 永久丢失。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>核对运行时 slot owner 仍指向当前 history entry，拒绝陈旧内存投影。</li>
     *     <li>短读 page3 header，校验 history base 与 removal lease 快照一致。</li>
     *     <li>核对 page3 队首、slot owner 和当前 entry first page 三者相同。</li>
     *     <li>读取 removed node，校验 committed UPDATE identity、队首链接和 EMPTY logical head。</li>
     *     <li>读取可选 successor 并校验 history length 与双向链接，冻结最终写批次输入。</li>
     * </ol>
     *
     * @param entry 当前运行时 committed history 队首；其 slot/first-page/creator/transactionNo 必须仍为权威 owner。
     * @param lease 由 {@link HistoryList#beginHeadRemoval(HistoryEntry)} 取得的队首转换 lease，阻止并发摘链。
     * @return 包含 page3 base、removed node 和可选 successor 的不可变 purge 预检结果。
     * @throws UndoLogFormatException slot/page3/history identity、EMPTY head、长度或双向链接任一不一致时抛出。
     * @throws RuntimeException page3/undo node 读取或短 MTR 提交失败时抛出；未发生 owner 转移或 segment drop。
     */
    private PreparedPurge preparePurge(HistoryEntry entry, HistoryList.HeadRemovalLease lease) {
        // 1. 内存 slot 投影必须仍属于当前 entry，避免用陈旧 lease 终结已复用的 undo segment。
        PageId memoryOwner = slotManager.undoFirstPageId(entry.slotId());
        if (!memoryOwner.equals(entry.undoFirstPageId())) {
            throw new UndoLogFormatException("memory rseg slot owner mismatch before purge unlink");
        }
        // 2. page3 只读快照提供持久 slot/history base；读 MTR 返回后不携带 page3 latch/fix。
        RollbackSegmentHeaderSnapshot header;
        MiniTransaction page3Read = mtrManager.beginReadOnly();
        try {
            header = headerRepository.read(page3Read, entry.undoSpaceId(), slotManager.rollbackSegmentId(),
                    slotManager.slotCapacity(), reuseDirectory.capacityPerKind());
            mtrManager.commit(page3Read);
        } catch (RuntimeException error) {
            rollbackActiveMtr(page3Read, error);
            throw error;
        }
        requireHistoryBaseMatchesLease(header.historyBase(), lease);
        // 3. page3 队首和 slot owner 必须同时指向 removed first page，不能只匹配其中一项。
        if (!entry.undoFirstPageId().equals(header.occupiedSlots().get(entry.slotId()))
                || !header.historyBase().headPageId().equals(Optional.of(entry.undoFirstPageId()))) {
            throw new UndoLogFormatException("persistent purge head/slot owner differs from runtime entry");
        }
        // 4. 只有逻辑链已完全消费的 committed UPDATE head 才能回收；非空 head 表示仍有记录级 purge 工作。
        UndoHistoryNodeSnapshot removed = inspectHistoryNode(entry.undoFirstPageId());
        if (!removed.isCommitted() || removed.kind() != UndoLogKind.UPDATE
                || !removed.creatorTransactionId().equals(entry.creatorTrxId())
                || !removed.committedTransactionNo().equals(entry.transactionNo())
                || removed.previousHistoryPageId().isPresent()
                || !removed.logicalHead().isEmpty()) {
            throw new UndoLogFormatException("persistent purge head identity/state mismatch");
        }
        // 5. successor 存在性必须与持久 length 一致，其 previous link 必须反向指回 removed head。
        Optional<UndoHistoryNodeSnapshot> newHead = removed.nextHistoryPageId().map(this::inspectHistoryNode);
        if ((header.historyBase().length() == 1L) != newHead.isEmpty()) {
            throw new UndoLogFormatException("persistent purge head successor disagrees with history length");
        }
        if (newHead.isPresent()) {
            UndoHistoryNodeSnapshot next = newHead.orElseThrow();
            if (!next.isCommitted() || next.kind() != UndoLogKind.UPDATE
                    || !next.previousHistoryPageId().equals(Optional.of(removed.firstPageId()))) {
                throw new UndoLogFormatException("persistent history successor does not point back to head");
            }
        }
        return new PreparedPurge(header.historyBase(), header.freeListBase(), removed, newHead);
    }

    /**
     * 在最终写批次前读取 inode 权威规模，用于把 redo admission 与实际 fragment/extent 数量绑定。
     * 该 MTR 与 page3/undo 预检严格分离，提交返回后只保留不可变值对象，避免 page2 latch 跨入 drop 写路径。
     */
    private UndoSegmentDropPlan inspectDropPlan(UndoSegmentHandle handle) {
        MiniTransaction planMtr = mtrManager.beginReadOnly();
        try {
            UndoSegmentDropPlan plan = undoAllocator.inspectDropPlan(planMtr, handle);
            mtrManager.commit(planMtr);
            return plan;
        } catch (RuntimeException error) {
            rollbackActiveMtr(planMtr, error);
            throw error;
        }
    }

    /**
     * 尝试为单 fragment 页 segment 取得 cache push lease。缓存容量满、drain 或同 kind transition 正忙时返回 null，
     * finalizer 沿用 drop；不等待 cache 短状态，避免终态 IO 被性能优化反向阻塞。
     */
    private UndoSegmentReuseDirectory.CachePushLease reserveCachePush(PreparedActive prepared,
                                                                  UndoSegmentDropPlan plan) {
        if (!isCacheEligible(prepared.handle(), plan)) {
            return null;
        }
        CachedUndoSegmentRef cached = new CachedUndoSegmentRef(prepared.binding().kind(), prepared.handle());
        return reuseDirectory.tryReserveCachePush(cached).orElse(null);
    }

    private ReusePushGroup reserveReusePushes(List<PreparedActive> prepared,
                                              List<UndoSegmentDropPlan> plans) {
        if (prepared.size() != plans.size()) {
            throw new DatabaseValidationException("undo finalization prepared/plan size mismatch");
        }
        List<UndoSegmentReuseDirectory.CachePushLease> acquired = new ArrayList<>();
        UndoSegmentReuseDirectory.FreePushLease freeLease = null;
        try {
            List<PreparedActive> freeCandidates = new ArrayList<>();
            for (int i = 0; i < prepared.size(); i++) {
                UndoSegmentReuseDirectory.CachePushLease push = reserveCachePush(prepared.get(i), plans.get(i));
                if (push != null) {
                    acquired.add(push);
                } else if (isCacheEligible(prepared.get(i).handle(), plans.get(i))) {
                    freeCandidates.add(prepared.get(i));
                }
            }
            if (!freeCandidates.isEmpty()) {
                List<FreeUndoSegmentRef> refs = freeCandidates.stream()
                        .map(item -> new FreeUndoSegmentRef(item.handle())).toList();
                freeLease = reuseDirectory.tryReserveFreePush(refs).orElse(null);
                if (freeLease != null) {
                    requireFreeProjectionMatchesPage3(freeCandidates, freeLease);
                }
            }
            UndoSegmentReuseDirectory.FreePushLease reservedFree = freeLease;
            List<FinalizationDisposition> dispositions = new ArrayList<>(prepared.size());
            for (int i = 0; i < prepared.size(); i++) {
                PreparedActive item = prepared.get(i);
                UndoSegmentReuseDirectory.CachePushLease cache = acquired.stream()
                        .filter(lease -> lease.segment().handle().equals(item.handle())).findFirst().orElse(null);
                boolean free = reservedFree != null && reservedFree.segments().stream()
                        .anyMatch(ref -> ref.handle().equals(item.handle()));
                dispositions.add(new FinalizationDisposition(item, plans.get(i), cache, free));
            }
            return new ReusePushGroup(dispositions, acquired, freeLease);
        } catch (RuntimeException failure) {
            if (freeLease != null) {
                try {
                    freeLease.close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            for (int i = acquired.size() - 1; i >= 0; i--) {
                try {
                    acquired.get(i).close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    /** 运行期 free tail/count 必须与所有预检看到的 page3 base 一致，不能把 stale 目录直接写回磁盘。
     *
     * @param candidates 参与 {@code requireFreeProjectionMatchesPage3} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param lease 调用方持有的 {@code UndoSegmentReuseDirectory.FreePushLease} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static void requireFreeProjectionMatchesPage3(List<PreparedActive> candidates,
                                                          UndoSegmentReuseDirectory.FreePushLease lease) {
        RollbackSegmentFreeListBase base = candidates.getFirst().freeBase();
        if (candidates.stream().anyMatch(item -> !item.freeBase().equals(base))
                || base.length() != lease.expectedCount()
                || !base.tailPageId().equals(lease.expectedTail()
                .map(item -> item.handle().firstPageId()))) {
            throw new UndoLogFormatException("runtime free projection differs from page3 base: base=" + base
                    + ", runtimeCount=" + lease.expectedCount() + ", runtimeTail=" + lease.expectedTail());
        }
    }

    private static boolean isCacheEligible(UndoSegmentHandle handle, UndoSegmentDropPlan plan) {
        return handle.firstPageId().equals(handle.lastPageId())
                && plan.usedPageCount() == 1L
                && plan.fragmentPageCount() == 1L
                && plan.extentCount() == 0L;
    }

    /**
     * 发布 finalization 的持久 owner：drop 目标清 active slot，cache/free 目标执行 active owner 转移；随后才按页号升序
     * 重置 cached first page。调用方已经先完成所有 FSP drop，因此整体页序为 page0/page2→page3→普通 undo 页。
     */
    private void publishFinalizationOwners(MiniTransaction mtr,
                                            List<FinalizationDisposition> dispositions) {
        List<RollbackSegmentHeaderRepository.CachePush> pushes = dispositions.stream()
                .filter(item -> item.cachePush() != null)
                .map(item -> new RollbackSegmentHeaderRepository.CachePush(
                        item.prepared().binding().slotId(), item.prepared().binding().firstPageId(),
                        item.prepared().binding().kind(), item.cachePush().expectedCount()))
                .toList();
        if (!pushes.isEmpty()) {
            headerRepository.moveActiveSlotsToCache(mtr,
                    dispositions.getFirst().prepared().binding().firstPageId().spaceId(), pushes);
        }
        List<FinalizationDisposition> free = dispositions.stream()
                .filter(FinalizationDisposition::free).toList();
        if (!free.isEmpty()) {
            RollbackSegmentFreeListBase expected = free.getFirst().prepared().freeBase();
            headerRepository.moveActiveSlotsToFree(mtr,
                    free.getFirst().prepared().binding().firstPageId().spaceId(), expected,
                    free.stream().map(item -> new RollbackSegmentHeaderRepository.FreePush(
                            item.prepared().binding().slotId(), item.prepared().binding().firstPageId())).toList());
        }
        for (FinalizationDisposition item : dispositions.stream()
                .filter(candidate -> !candidate.retained())
                .sorted(Comparator.comparingInt(candidate -> candidate.prepared().binding().slotId().value()))
                .toList()) {
            UndoLogBinding binding = item.prepared().binding();
            headerRepository.clearSlot(mtr, binding.firstPageId().spaceId(),
                    binding.slotId(), binding.firstPageId());
        }
    }

    /** 只在 MTR 仍 ACTIVE 时释放 memo；COMMITTING 失败保持原始结果不确定原因。
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
     * 封装事务、MVCC 与锁中 {@code PreparedActive} 已校验但尚待发布的事务阶段状态；字段共同固定 owner、物理证据和补偿边界，防止提交/回滚重复执行。
     *
     * @param binding 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param freeBase 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    private record PreparedActive(UndoLogBinding binding, UndoSegmentHandle handle,
                                  RollbackSegmentFreeListBase freeBase) {
        private PreparedActive {
            if (binding == null || handle == null || freeBase == null) {
                throw new DatabaseValidationException("prepared active undo fields must not be null");
            }
        }
    }

    /**
     * 封装事务、MVCC 与锁中 {@code PreparedHistoryAppend} 已校验但尚待发布的事务阶段状态；字段共同固定 owner、物理证据和补偿边界，防止提交/回滚重复执行。
     *
     * @param update 当前算法已准备的中间状态；不得为 {@code null}，必须由本次扫描、日志组装或事务终结流程创建且尚未发布
     * @param base 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param oldTail 可选的 {@code oldTail}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     */
    private record PreparedHistoryAppend(PreparedActive update, RollbackSegmentHistoryBase base,
                                         Optional<UndoHistoryNodeSnapshot> oldTail) {
        private PreparedHistoryAppend {
            if (update == null || base == null || oldTail == null) {
                throw new DatabaseValidationException("prepared history append fields must not be null");
            }
        }
    }

    /**
     * 封装事务、MVCC 与锁中 {@code PreparedPurge} 已校验但尚待发布的事务阶段状态；字段共同固定 owner、物理证据和补偿边界，防止提交/回滚重复执行。
     *
     * @param base 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param freeBase 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param removed 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param newHead 可选的 {@code newHead}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     */
    private record PreparedPurge(RollbackSegmentHistoryBase base, RollbackSegmentFreeListBase freeBase,
                                 UndoHistoryNodeSnapshot removed,
                                 Optional<UndoHistoryNodeSnapshot> newHead) {
        private PreparedPurge {
            if (base == null || freeBase == null || removed == null || newHead == null) {
                throw new DatabaseValidationException("prepared purge fields must not be null");
            }
        }
    }

    /**
     * 封装事务、MVCC 与锁中 {@code FinalizationDisposition} 已校验但尚待发布的事务阶段状态；字段共同固定 owner、物理证据和补偿边界，防止提交/回滚重复执行。
     *
     * @param prepared 当前算法已准备的中间状态；不得为 {@code null}，必须由本次扫描、日志组装或事务终结流程创建且尚未发布
     * @param dropPlan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param cachePush 调用方持有的 {@code UndoSegmentReuseDirectory.CachePushLease} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param free 资源是否处于删除、空闲、静默、持久化或终态；必须与权威状态机一致，不能由调用方猜测
     */
    private record FinalizationDisposition(PreparedActive prepared, UndoSegmentDropPlan dropPlan,
                                           UndoSegmentReuseDirectory.CachePushLease cachePush,
                                           boolean free) {
        private FinalizationDisposition {
            if (prepared == null || dropPlan == null) {
                throw new DatabaseValidationException("undo finalization disposition fields must not be null");
            }
            if (cachePush != null && free) {
                throw new DatabaseValidationException("undo segment cannot enter cache and free simultaneously");
            }
        }

        private boolean retained() {
            return cachePush != null || free;
        }
    }

    /** cache/free push lease 的 RAII 组合；物理写前统一立 fence，commit 后统一发布运行期 owner。 */
    private static final class ReusePushGroup implements AutoCloseable {
        /**
         * 本对象拥有的 {@code dispositions} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
        private final List<FinalizationDisposition> dispositions;
        /**
         * 本对象拥有的 {@code leases} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
        private final List<UndoSegmentReuseDirectory.CachePushLease> leases;
        /**
         * 本次事务链路持有的 {@code freeLease} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
         */
        private final UndoSegmentReuseDirectory.FreePushLease freeLease;

        private ReusePushGroup(List<FinalizationDisposition> dispositions,
                               List<UndoSegmentReuseDirectory.CachePushLease> leases,
                               UndoSegmentReuseDirectory.FreePushLease freeLease) {
            this.dispositions = List.copyOf(dispositions);
            this.leases = List.copyOf(leases);
            this.freeLease = freeLease;
        }

        private List<FinalizationDisposition> dispositions() {
            return dispositions;
        }

        private Optional<FreeUndoSegmentRef> oldFreeTail() {
            return freeLease == null ? Optional.empty() : freeLease.expectedTail();
        }

        private void physicalMutationStarted() {
            for (UndoSegmentReuseDirectory.CachePushLease lease : leases) {
                lease.physicalMutationStarted();
            }
            if (freeLease != null) {
                freeLease.physicalMutationStarted();
            }
        }

        private void complete() {
            for (UndoSegmentReuseDirectory.CachePushLease lease : leases) {
                lease.complete();
            }
            if (freeLease != null) {
                freeLease.complete();
            }
        }

        /**
         * 释放本方法拥有的事务、MVCC 与锁资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
         * <p>数据流：</p>
         * <ol>
         *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
         *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
         *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
         *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
         * </ol>
         *
         */
        @Override
        public void close() {
            // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
            RuntimeException failure = null;
            // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
            if (freeLease != null) {
                try {
                    freeLease.close();
                } catch (RuntimeException closeFailure) {
                    failure = closeFailure;
                }
            }
            // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
            for (int i = leases.size() - 1; i >= 0; i--) {
                try {
                    leases.get(i).close();
                } catch (RuntimeException closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
            if (failure != null) {
                throw failure;
            }
        }
    }
}
