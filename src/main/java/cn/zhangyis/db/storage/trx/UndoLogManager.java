package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.UndoAppendSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordType;
import cn.zhangyis.db.storage.undo.UndoRecordWritePlan;
import cn.zhangyis.db.storage.undo.UndoSpaceReservation;

import java.util.List;

/**
 * 事务 undo 门面（设计 §5.2/§7.1/§7.2）。在 {@code storage.trx} 持事务语义，调用
 * {@code storage.undo} 物理设施（{@link UndoLogSegmentAccess}）写 INSERT/UPDATE/DELETE_MARK undo record，
 * 返回真 {@link RollPointer} 供聚簇记录盖 {@code DB_ROLL_PTR}，并在 commit/rollback/recovery/purge
 * 流程中维护 undo slot 与 history list。
 *
 * <p><b>依赖方向</b>：{@code storage.trx → storage.undo}。本类 import {@link MiniTransaction}（物理短事务）、
 * {@link UndoLogSegmentAccess}/{@link UndoRecord}（undo 物理设施）与 record schema/type；不反向暴露
 * {@link Transaction} 内部给 undo。{@code storage.undo} 不 import 本类或 {@code Transaction}。
 *
 * <p><b>当前范围</b>：生产 DML 与测试协作者统一通过
 * {@code planInsert/planUpdate/planDelete + appendPlanned} 在业务 MTR admission 前冻结目标 kind、持久头快照、
 * inline/external 编码、精确 reservation 与 redo workload。INSERT/UPDATE 首写分别惰性创建独立 slot/segment；
 * prepared transaction 与多 rseg 调度仍留后续切片。
 *
 * <p><b>WAL/失败边界</b>（§7.2）：生产 append 与聚簇修改仍在同一 MTR，undo root/payload redo 与 index redo
 * 同批提交。stale/编码/配置错误在 reservation 前拒绝；进入 segment/payload 物理修改后，或 undo 已写而聚簇修改
 * 失败，统一抛 {@link UndoWriteFatalException}，因为 MTR rollback 不撤销 buffer content，调用方不得在同进程重试。
 *
 * <p><b>并发</b>：slot 认领由 {@link RollbackSegmentSlotManager} 的 {@code ReentrantLock} 串行，锁内不分配页、
 * 不访问 BufferPool、不等待 IO；页分配（{@link UndoLogSegmentAccess#create}）在 slot 锁外完成。本片单 writer
 * 假设：同一事务/undo segment 同时只有一个 EXCLUSIVE append 会话。
 */
public final class UndoLogManager {

    /** undo 物理设施入口；生产 plan/append 均经它 create/open/resolve undo segment。 */
    private final UndoLogSegmentAccess access;
    /** 内存 rseg slot 目录；每个 kind 首写时各认领一个 slot。固定单一默认 rseg。 */
    private final RollbackSegmentSlotManager slotManager;
    /** undo 表空间；目标 kind 尚无 binding 时由 planned append 在此空间惰性建段。 */
    private final SpaceId undoSpace;
    /** 已提交 update/delete undo 的 history list；纯 insert undo 在 commit finalization 中按条件缓存或回收。 */
    private final HistoryList history;
    /**
     * 持久 rseg header 仓储。首写 claim 与 undo segment 创建同一 MTR；终态 clear 统一交给 finalizer。
     */
    private final RollbackSegmentHeaderRepository headerRepo;
    /** commit/rollback/purge 共享的 atomic cache/drop + page3 owner 转移协作者。 */
    private final UndoSegmentFinalizer finalizer;
    /** page3 cached INSERT/UPDATE segment 的运行期 LIFO 投影。 */
    private final UndoSegmentCacheDirectory cacheDirectory;

    /**
     * 构造生产 undo 门面。slot claim 必须持久到 page3；纯 insert commit 必须经 finalizer 在同一 redo batch
     * cache/drop segment + 转移 page3 owner，故不再提供绕过持久生命周期的构造方式。
     *
     * @param access      undo 物理设施入口，不能为 null。
     * @param slotManager 内存 rseg slot 目录，不能为 null。
     * @param undoSpace   undo 表空间，不能为 null。
     * @param history     已提交 undo log 的 history list，不能为 null。
     * @param headerRepo  持久 rseg header 仓储。
     * @param finalizer   atomic undo segment 终态协作者。
     */
    public UndoLogManager(UndoLogSegmentAccess access, RollbackSegmentSlotManager slotManager, SpaceId undoSpace,
                           HistoryList history, RollbackSegmentHeaderRepository headerRepo,
                           UndoSegmentFinalizer finalizer, UndoSegmentCacheDirectory cacheDirectory) {
        if (access == null || slotManager == null || undoSpace == null || history == null
                || headerRepo == null || finalizer == null || cacheDirectory == null) {
            throw new DatabaseValidationException("undo log manager args must not be null");
        }
        this.access = access;
        this.slotManager = slotManager;
        this.undoSpace = undoSpace;
        this.history = history;
        this.headerRepo = headerRepo;
        this.finalizer = finalizer;
        this.cacheDirectory = cacheDirectory;
    }

    /** 在写 MTR admission 前规划 INSERT undo。 */
    public UndoWritePlan planInsert(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, IndexKeyDef keyDef, TableSchema schema) {
        if (clusterKey == null || keyDef == null || schema == null) {
            throw new TransactionStateException("planInsert clusterKey/keyDef/schema must not be null");
        }
        PlanningContext context = planningContext(txn, UndoLogKind.INSERT);
        UndoRecord record = UndoRecord.insert(context.nextUndoNo(), context.transactionId(), tableId, indexId,
                clusterKey, context.logicalHead().rollPointer());
        return buildPlan(context, record, keyDef, schema);
    }

    /** 在写 MTR admission 前规划 UPDATE undo，并冻结完整旧行 image 编码。 */
    public UndoWritePlan planUpdate(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns, IndexKeyDef keyDef, TableSchema schema) {
        if (clusterKey == null || oldColumnValues == null || oldHiddenColumns == null
                || keyDef == null || schema == null) {
            throw new TransactionStateException("planUpdate old image/key/schema args must not be null");
        }
        PlanningContext context = planningContext(txn, UndoLogKind.UPDATE);
        UndoRecord record = UndoRecord.update(context.nextUndoNo(), context.transactionId(), tableId, indexId,
                clusterKey, oldColumnValues, oldHiddenColumns, context.logicalHead().rollPointer());
        return buildPlan(context, record, keyDef, schema);
    }

    /** 在写 MTR admission 前规划 DELETE_MARK undo，并冻结删除前存活版本。 */
    public UndoWritePlan planDelete(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns, IndexKeyDef keyDef, TableSchema schema) {
        if (clusterKey == null || oldColumnValues == null || oldHiddenColumns == null
                || keyDef == null || schema == null) {
            throw new TransactionStateException("planDelete old image/key/schema args must not be null");
        }
        PlanningContext context = planningContext(txn, UndoLogKind.UPDATE);
        UndoRecord record = UndoRecord.deleteMark(context.nextUndoNo(), context.transactionId(), tableId, indexId,
                clusterKey, oldColumnValues, oldHiddenColumns, context.logicalHead().rollPointer());
        return buildPlan(context, record, keyDef, schema);
    }

    /**
     * 在调用方已经按 {@link UndoWritePlan#redoWorkload()} 完成 admission 的 MTR 中执行计划。所有 stale 校验先于
     * reservation；进入 reservation/append 后任意失败转为 fatal，因为 MTR rollback 不撤销 buffer content。
     */
    public RollPointer appendPlanned(Transaction txn, MiniTransaction mtr, UndoWritePlan plan) {
        if (txn == null || mtr == null || plan == null) {
            throw new TransactionStateException("appendPlanned txn/mtr/plan must not be null");
        }
        requireActiveTransaction(txn);
        if (!txn.transactionId().equals(plan.transactionId())) {
            throw new UndoWriteStalePlanException("undo plan transaction id no longer matches target transaction");
        }
        return switch (plan.acquisition()) {
            case ALLOCATE_NEW -> appendAllocatedLogPlanned(txn, mtr, plan);
            case REUSE_CACHED -> appendCachedLogPlanned(txn, mtr, plan);
            case APPEND_EXISTING -> appendExistingPlanned(txn, mtr, plan);
        };
    }

    private UndoWritePlan buildPlan(PlanningContext context, UndoRecord record,
                                    IndexKeyDef keyDef, TableSchema schema) {
        UndoRecordWritePlan physical = access.planRecord(record, keyDef, schema);
        int pages = switch (context.acquisition()) {
            case ALLOCATE_NEW -> access.plannedNewPages(true, null, physical);
            case REUSE_CACHED -> physical.externalPageCount();
            case APPEND_EXISTING -> access.plannedNewPages(false, context.persistentSnapshot(), physical);
        };
        return new UndoWritePlan(context.transactionId(), context.kind(), context.acquisition(), context.firstPageId(),
                context.globalLastUndoNo(), context.logicalHead(), context.persistentSnapshot(),
                context.cachedCandidate(), physical, pages,
                UndoRedoBudgetEstimator.append(context.acquisition(), physical.externalPageCount()));
    }

    private PlanningContext planningContext(Transaction txn, UndoLogKind kind) {
        requireActiveTransaction(txn);
        TransactionId txnId = txn.transactionId();
        UndoContext context = txn.undoContext();
        UndoNo globalLast = context == null ? UndoNo.NONE : context.lastUndoNo();
        UndoLogBinding binding = context == null ? null : context.binding(kind);
        if (binding == null) {
            UndoSegmentCacheDirectory.CacheCandidate candidate = cacheDirectory.peek(kind).orElse(null);
            UndoSegmentAcquisition acquisition = candidate == null
                    ? UndoSegmentAcquisition.ALLOCATE_NEW : UndoSegmentAcquisition.REUSE_CACHED;
            PageId firstPage = candidate == null ? null : candidate.segment().handle().firstPageId();
            return new PlanningContext(txnId, kind, acquisition, firstPage, globalLast,
                    UndoLogicalHead.EMPTY, null, candidate);
        }
        UndoAppendSnapshot snapshot = binding.appendSnapshot();
        if (snapshot == null || !snapshot.transactionId().equals(txnId)
                || !snapshot.firstPageId().equals(binding.firstPageId())
                || snapshot.kind() != kind
                || !snapshot.logicalHead().equals(binding.logicalHead())) {
            throw new UndoWriteStalePlanException("transaction undo binding lacks a current append snapshot");
        }
        return new PlanningContext(txnId, kind, UndoSegmentAcquisition.APPEND_EXISTING,
                binding.firstPageId(), globalLast, binding.logicalHead(), snapshot, null);
    }

    private RollPointer appendAllocatedLogPlanned(Transaction txn, MiniTransaction mtr, UndoWritePlan plan) {
        UndoContext current = txn.undoContext();
        UndoNo currentGlobal = current == null ? UndoNo.NONE : current.lastUndoNo();
        if (!currentGlobal.equals(plan.expectedGlobalLastUndoNo())
                || (current != null && current.hasBinding(plan.kind()))) {
            throw new UndoWriteStalePlanException("new undo log plan is stale because transaction state changed");
        }
        try (RollbackSegmentSlotManager.ClaimLease claim = slotManager.reserveClaim()) {
            requirePersistentSlotFree(mtr, claim.slotId());
            UndoSpaceReservation reservation = access.reservePages(mtr, undoSpace, plan.pagesToReserve());
            claim.physicalMutationStarted();
            try (reservation) {
                    UndoLogSegment segment = access.create(mtr, undoSpace, plan.transactionId(), plan.kind());
                    if (segment.requiredNewPages(plan.recordPlan()) != plan.recordPlan().externalPageCount()) {
                        throw new UndoWriteFatalException("new undo segment page requirement differs from plan");
                    }
                    RollPointer pointer = segment.appendPlanned(plan.recordPlan());
                    PageId firstPageId = segment.firstPageId();
                    claim.bind(firstPageId);
                    claimRsegSlotAfterUndoPage(mtr, claim.slotId(), firstPageId);
                    UndoContext context = current == null
                            ? new UndoContext(slotManager.rollbackSegmentId()) : current;
                    context.attach(new UndoLogBinding(plan.kind(), claim.slotId(), firstPageId,
                            UndoLogicalHead.EMPTY));
                    publishContextAfterAppend(txn, context, plan, pointer, segment.appendSnapshot());
                    return pointer;
            } catch (RuntimeException error) {
                throw fatalAfterMutation("first undo write failed after physical mutation started", error);
            }
        }
    }

    /**
     * 用 page3 cached top 开启事务的新 kind-local undo log。cache pop、active slot claim、首页激活与首条 append
     * 处于同一业务 MTR；进入物理边界后的任何异常均 fail-stop，不能把同一 inode 同时重新发布到 cache/active。
     */
    private RollPointer appendCachedLogPlanned(Transaction txn, MiniTransaction mtr, UndoWritePlan plan) {
        UndoContext current = txn.undoContext();
        UndoNo currentGlobal = current == null ? UndoNo.NONE : current.lastUndoNo();
        if (!currentGlobal.equals(plan.expectedGlobalLastUndoNo())
                || (current != null && current.hasBinding(plan.kind()))) {
            throw new UndoWriteStalePlanException("cached undo plan is stale because transaction state changed");
        }
        try (RollbackSegmentSlotManager.ClaimLease claim = slotManager.reserveClaim();
             UndoSegmentCacheDirectory.PopLease cachePop = cacheDirectory.reservePop(plan.cachedCandidate())) {
            requirePersistentSlotFree(mtr, claim.slotId());
            UndoSpaceReservation reservation = plan.pagesToReserve() == 0
                    ? null : access.reservePages(mtr, undoSpace, plan.pagesToReserve());
            try (reservation) {
                claim.physicalMutationStarted();
                cachePop.physicalMutationStarted();
                try {
                    var candidate = cachePop.candidate();
                    headerRepo.moveCachedTopToActiveSlot(mtr, undoSpace, plan.kind(),
                            candidate.expectedCount(), candidate.segment().handle().firstPageId(), claim.slotId());
                    UndoLogSegment segment = access.activateCached(mtr, candidate.segment(), plan.transactionId());
                    if (segment.requiredNewPages(plan.recordPlan()) != plan.pagesToReserve()) {
                        throw new UndoWriteFatalException("cached undo segment page requirement differs from plan");
                    }
                    RollPointer pointer = segment.appendPlanned(plan.recordPlan());
                    PageId firstPageId = segment.firstPageId();
                    claim.bind(firstPageId);
                    cachePop.complete();
                    UndoContext context = current == null
                            ? new UndoContext(slotManager.rollbackSegmentId()) : current;
                    context.attach(new UndoLogBinding(plan.kind(), claim.slotId(), firstPageId,
                            UndoLogicalHead.EMPTY));
                    publishContextAfterAppend(txn, context, plan, pointer, segment.appendSnapshot());
                    return pointer;
                } catch (RuntimeException error) {
                    throw fatalAfterMutation("cached undo first write failed after owner transition began", error);
                }
            }
        }
    }

    private RollPointer appendExistingPlanned(Transaction txn, MiniTransaction mtr, UndoWritePlan plan) {
        UndoContext context = txn.undoContext();
        requireMatchingContext(context, plan);
        // 预留必须先于 undo 尾页 X latch；FSP page0/page2 的页号低于普通 undo 页，反序会形成统一 latch-order 违规。
        UndoSpaceReservation reservation = plan.pagesToReserve() == 0
                ? null
                : access.reservePages(mtr, undoSpace, plan.pagesToReserve());
        try (reservation) {
            UndoLogSegment segment = access.open(mtr, plan.expectedFirstPageId(), PageLatchMode.EXCLUSIVE);
            if (!segment.appendSnapshot().equals(plan.persistentSnapshot())) {
                throw new UndoWriteStalePlanException("persistent undo append snapshot changed before execution");
            }
            int actualPages = segment.requiredNewPages(plan.recordPlan());
            if (actualPages != plan.pagesToReserve()) {
                throw new UndoWriteStalePlanException("undo root placement changed before execution");
            }
            RollPointer pointer = segment.appendPlanned(plan.recordPlan());
            publishContextAfterAppend(txn, context, plan, pointer, segment.appendSnapshot());
            return pointer;
        } catch (RuntimeException error) {
            throw fatalAfterMutation("existing undo write failed after physical mutation started", error);
        }
    }

    private void publishContextAfterAppend(Transaction txn, UndoContext context,
                                           UndoWritePlan plan, RollPointer pointer,
                                           UndoAppendSnapshot snapshot) {
        context.publishAppend(plan.kind(), plan.recordPlan().record().undoNo(), pointer, snapshot);
        if (txn.undoContext() == null) {
            txn.setUndoContext(context);
        }
    }

    private static void requireMatchingContext(UndoContext context, UndoWritePlan plan) {
        UndoLogBinding binding = context == null ? null : context.binding(plan.kind());
        if (binding == null || !binding.firstPageId().equals(plan.expectedFirstPageId())
                || !context.lastUndoNo().equals(plan.expectedGlobalLastUndoNo())
                || !binding.logicalHead().equals(plan.expectedLogicalHead())) {
            throw new UndoWriteStalePlanException("transaction undo context changed before planned append");
        }
    }

    private static UndoWriteFatalException fatalAfterMutation(String message, RuntimeException error) {
        if (error instanceof UndoWriteFatalException fatal) {
            return fatal;
        }
        return new UndoWriteFatalException(message, error);
    }

    private static void requireActiveTransaction(Transaction txn) {
        if (txn == null) {
            throw new TransactionStateException("undo planning transaction must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("undo write requires ACTIVE transaction: " + txn.state());
        }
        if (txn.transactionId().isNone()) {
            throw new TransactionStateException("undo write requires assigned transaction id");
        }
    }

    /** 规划期冻结的事务链入口。 */
    private record PlanningContext(TransactionId transactionId, UndoLogKind kind,
                                   UndoSegmentAcquisition acquisition,
                                    PageId firstPageId, UndoNo globalLastUndoNo, UndoLogicalHead logicalHead,
                                    UndoAppendSnapshot persistentSnapshot,
                                    UndoSegmentCacheDirectory.CacheCandidate cachedCandidate) {
        private UndoNo nextUndoNo() {
            if (globalLastUndoNo.value() == Long.MAX_VALUE) {
                throw new TransactionStateException("undo number exhausted at Long.MAX_VALUE");
            }
            return UndoNo.of(globalLastUndoNo.value() + 1L);
        }
    }

    /**
     * 提交 undo 生命周期（对齐 InnoDB {@code trx_undo_insert_cleanup} / history 挂接思想）。数据流：
     * <ul>
     *   <li>未写事务（{@code undoContext()==null}）：no-op。</li>
     *   <li>INSERT-only：单 fragment 段尝试进入 INSERT cache，其余 drop；同时写正式 commit 终态。</li>
     *   <li>UPDATE-only：把 UPDATE log 标为 COMMITTED 并挂 history，留给 MVCC/purge。</li>
     *   <li>mixed：同一 MTR cache/drop INSERT、标记 UPDATE COMMITTED，并只写一次 commit 终态。</li>
     * </ul>
     *
     * <p><b>commit 编排</b>：{@code TransactionManager.commit()} 保持纯内存状态、**不**自动调用本方法；2.1 起
     * {@code ClusteredDmlService.commit} 先通过 {@code TransactionManager.prepareCommit(txn)} 预留提交号，
     * 再调用本方法持久化 undo 终态，最后才 {@code commit(txn)} 移出 active table。纯 insert 在此缓存或回收；
     * update/delete 原子更新 page3 base 与 first-page links 后才发布内存 history 投影，保留给 MVCC/purge。
     *
     * @param txn 提交中的事务，不能为 null。
     */
    public void onCommit(Transaction txn) {
        if (txn == null) {
            throw new TransactionStateException("onCommit txn must not be null");
        }
        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            return; // 未写事务：无 undo 段
        }
        TransactionNo no = txn.transactionNo();
        if (no.isNone()) {
            throw new TransactionStateException(
                    "onCommit requires an assigned TransactionNo; call TransactionManager.prepareCommit first");
        }
        UndoLogBinding update = ctx.binding(UndoLogKind.UPDATE);
        if (update == null) {
            finalizer.finalizeCommit(txn, ctx, null);
            return;
        }
        HistoryEntry entry = new HistoryEntry(no, txn.transactionId(), undoSpace,
                update.firstPageId(), update.slotId());
        // transition 在任何 page/FSP 写前取得；timeout/interrupt 不会留下半持久 commit，可由上层重试。
        try (HistoryList.AppendLease lease = history.beginAppend(entry)) {
            finalizer.finalizeCommit(txn, ctx, lease);
        }
    }

    private void requirePersistentSlotFree(MiniTransaction mtr, UndoSlotId slot) {
        headerRepo.requireSlotFree(mtr, undoSpace, slot);
    }

    /**
     * 持久化 rseg slot 的 page-latch-order 例外。首写和提交清理都已经在同一 MTR 持有 undo first 页 X latch；
     * 该页可能被格式化、append 或 markCommitted 写过，不能提前释放，否则 MTR commit 盖 pageLSN 会失去 X guard。
     *
     * <p>局部无环前提：rseg header(page3) 只记录 slot->firstPageNo 映射，不会读取/等待 undo 页内容；undo
     * segment 的普通写路径也不会在持 page3 latch 时反向请求 undo page latch。因此该例外只放在 UndoLogManager
     * 的事务 undo 编排层，不下沉到 repository 的所有调用。
     */
    private void claimRsegSlotAfterUndoPage(MiniTransaction mtr, UndoSlotId slot, PageId firstPage) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "undo rseg slot update: page3 metadata never waits for undo page latches")) {
            headerRepo.claimSlot(mtr, undoSpace, slot, firstPage);
        }
    }
}
