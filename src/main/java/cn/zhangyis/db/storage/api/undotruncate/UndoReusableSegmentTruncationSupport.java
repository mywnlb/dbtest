package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.trx.UndoRedoBudgetEstimator;
import cn.zhangyis.db.storage.trx.UndoSegmentReuseDirectory;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.FreeUndoSegmentRef;
import cn.zhangyis.db.storage.undo.RollbackSegmentFreeListBase;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * UNDO truncate 与可复用 segment 目录之间的内部协作者。调用方必须已持目标表空间 lifecycle X lease；本类只用
 * 非阻塞统一 drain gate，并把每批最多八个 cache/free segment 的 FSP drop 与 page3 owner removal 放入同一 MTR。
 *
 * <p>锁序是：短只读 undo 首页校验并释放 → 短只读 FSP inode 校验并释放 → 写 MTR 依次修改 page0/page2/page3。
 * 任何 active slot 都在首批 drop 之前拒绝，避免 truncate 回收仍属于事务的 segment。
 */
class UndoReusableSegmentTruncationSupport {

    /** 限制单次 truncate MTR 的 FSP/page3 修改规模，避免 redo batch 随可复用目录无界增长。 */
    private static final int MAX_SEGMENTS_PER_MTR = 8;

    /** truncate 校验与物理批次的 MTR 来源。 */
    private final MiniTransactionManager mtrManager;
    /** cache/free 首页格式和 owner 校验入口。 */
    private final UndoLogSegmentAccess undoAccess;
    /** FSP inode 检查和 segment drop 入口。 */
    private final UndoSpaceAllocator allocator;
    /** page3 active/cache/free owner 的持久仓储。 */
    private final RollbackSegmentHeaderRepository headerRepository;
    /** 当前实现唯一 rollback segment 的稳定标识。 */
    private final RollbackSegmentId rollbackSegmentId;
    /** page3 active slot 格式容量，truncate rebuild 必须原样复用。 */
    private final int slotCapacity;
    /** page3 每类 cache 格式容量，必须和运行期目录一致。 */
    private final int cacheCapacityPerKind;
    /** page3 cache/free owner 的统一运行期投影。 */
    private final UndoSegmentReuseDirectory reuseDirectory;

    /**
     * 创建统一 reuse drain 协作者；所有依赖必须来自同一 StorageEngine 组合根，容量必须与 page3 和运行期目录一致。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param mtrManager drain 读写 MTR 来源。
     * @param undoAccess cache/free 首页格式校验入口。
     * @param allocator FSP inode 检查与 segment drop 入口。
     * @param headerRepository page3 active/cache/free owner 仓储。
     * @param rollbackSegmentId 当前唯一 rollback segment id。
     * @param slotCapacity page3 active slot 持久容量。
     * @param cacheCapacityPerKind page3 每类 cache 持久容量。
     * @param reuseDirectory 与 page3 对应的运行期可复用目录投影。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoReusableSegmentTruncationSupport(
            MiniTransactionManager mtrManager,
            UndoLogSegmentAccess undoAccess,
            UndoSpaceAllocator allocator,
            RollbackSegmentHeaderRepository headerRepository,
            RollbackSegmentId rollbackSegmentId,
            int slotCapacity,
            int cacheCapacityPerKind,
            UndoSegmentReuseDirectory reuseDirectory) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (mtrManager == null || undoAccess == null || allocator == null || headerRepository == null
                || rollbackSegmentId == null || reuseDirectory == null) {
            throw new DatabaseValidationException("undo reusable-segment truncation dependencies must not be null");
        }
        if (slotCapacity <= 0 || cacheCapacityPerKind < 0
                || reuseDirectory.capacityPerKind() != cacheCapacityPerKind) {
            throw new DatabaseValidationException("undo reusable-segment truncation capacities are invalid");
        }
        this.mtrManager = mtrManager;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.undoAccess = undoAccess;
        this.allocator = allocator;
        this.headerRepository = headerRepository;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.rollbackSegmentId = rollbackSegmentId;
        this.slotCapacity = slotCapacity;
        this.cacheCapacityPerKind = cacheCapacityPerKind;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.reuseDirectory = reuseDirectory;
    }

    /**
     * stable truncate 在 marker 前执行：非阻塞封锁运行期 reuse directory，先拒绝 persistent history/active slots，
     * 再按 INSERT cache、UPDATE cache、free FIFO 顺序逐批 drop owners。
     * gate 忙表示某 finalizer/reuser 已跨出短锁，调用方必须释放 lifecycle X 后重试，不能在 X 下等待。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @throws UndoTablespaceNotEmptyException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public void drainStableSpace(SpaceId spaceId) {
        UndoReusableSegmentDrainStatus status = tryDrainStableSpace(spaceId);
        switch (status) {
            case DRAINED -> {
                return;
            }
            case DEFERRED_HISTORY -> throw new UndoTablespaceNotEmptyException(
                    "UNDO rollback segment history is not empty: space=" + spaceId.value());
            case DEFERRED_ACTIVE_SLOTS -> throw new UndoTablespaceNotEmptyException(
                    "UNDO rollback segment still owns active slots: space=" + spaceId.value());
            case DEFERRED_REUSE_BUSY -> throw new UndoTablespaceTruncationException(
                    "undo reuse transition is in progress; release lifecycle X lease and retry truncate");
        }
    }

    /**
     * 为后台调度非阻塞地判断并排空 stable space reusable owner。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>零等待取得 reuse directory 全局 drain gate；竞争失败不读取或修改 page3。</li>
     *     <li>读取 page3 持久快照，history/active 任一非空即在物理修改前释放 gate并返回 deferred。</li>
     *     <li>交叉校验运行期 cache/free 投影，按有界批次 drop FSP owner 并原子更新 page3。</li>
     *     <li>复核持久 owner 全空后发布 drain 完成；越过物理边界的失败继续沿既有 fatal fence 抛出。</li>
     * </ol>
     *
     * @param spaceId 当前单 rseg 所属的 UNDO 表空间；不得为 {@code null}
     * @return DRAINED 或无物理副作用的具体 deferred 原因
     * @throws UndoReusableSegmentDrainException FSP/page3 修改越过物理边界后失败时抛出，owner 必须 fail-stop
     * @throws UndoTablespaceTruncationException owner 格式、运行期投影或持久证据不一致时抛出
     */
    UndoReusableSegmentDrainStatus tryDrainStableSpace(SpaceId spaceId) {
        // 1、候选只接受当前显式 undo space；gate busy 是正常并发结果，不能在 lifecycle X 下等待。
        requireSpace(spaceId);
        var optionalDrain = reuseDirectory.tryBeginDrain();
        if (optionalDrain.isEmpty()) {
            return UndoReusableSegmentDrainStatus.DEFERRED_REUSE_BUSY;
        }
        UndoSegmentReuseDirectory.DrainLease drain = optionalDrain.orElseThrow();
        try (drain) {
            // 2、page3 是 active/history 权威；返回前 close 会撤销尚未物理修改的 drain gate。
            RollbackSegmentHeaderSnapshot snapshot = readHeader(spaceId);
            if (snapshot.historyBase().length() != 0L) {
                return UndoReusableSegmentDrainStatus.DEFERRED_HISTORY;
            }
            if (!snapshot.occupiedSlots().isEmpty()) {
                return UndoReusableSegmentDrainStatus.DEFERRED_ACTIVE_SLOTS;
            }
            // 3、只有持久 active/history 全空才允许消费 cache/free；批次失败保留原有 fail-stop 语义。
            requireRuntimeStacksMatch(snapshot);

            while (true) {
                var optionalBatch = drain.nextBatch(MAX_SEGMENTS_PER_MTR);
                if (optionalBatch.isEmpty()) {
                    break;
                }
                drainBatch(spaceId, drain, optionalBatch.orElseThrow());
            }
            // 4、最终复核 page3 全空后才释放全局 gate，防止 marker 建立在残留 owner 之上。
            requirePersistentEmpty(spaceId);
            drain.finish();
            return UndoReusableSegmentDrainStatus.DRAINED;
        }
    }

    /**
     * durable TRUNCATING 恢复只能续作一个已经在 marker 前清空 owner 的空间。此时 transaction recovery 尚未重建
     * reuse directory，因此只读取 page3；任何 history/active/cache/free owner 都说明 marker 顺序被破坏，必须 fail-closed。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @throws UndoTablespaceNotEmptyException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public void requirePersistentEmpty(SpaceId spaceId) {
        RollbackSegmentHeaderSnapshot snapshot = readHeader(spaceId);
        if (!snapshot.occupiedSlots().isEmpty()
                || !snapshot.cachedInsertSegments().isEmpty()
                || !snapshot.cachedUpdateSegments().isEmpty()
                || snapshot.freeListBase().length() != 0L
                || snapshot.historyBase().length() != 0L) {
            throw new UndoTablespaceNotEmptyException("TRUNCATING UNDO space still has rollback segment owners: space="
                    + spaceId.value() + ", active=" + snapshot.occupiedSlots().size()
                    + ", insertCache=" + snapshot.cachedInsertSegments().size()
                    + ", updateCache=" + snapshot.cachedUpdateSegments().size()
                    + ", free=" + snapshot.freeListBase().length()
                    + ", history=" + snapshot.historyBase().length());
        }
    }

    /** 物理截断后在 page0/page2 重建同一 MTR 中重建 page3 v4 空目录。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     */
    public void rebuildHeader(MiniTransaction mtr, SpaceId spaceId) {
        headerRepository.format(mtr, spaceId, rollbackSegmentId, slotCapacity, cacheCapacityPerKind);
    }

    /**
     * 清理一个最多八段的冻结批次。数据先从运行期 lease 进入，逐段校验首页与 FSP 单 fragment 证据，再复核
     * page3/free base；写 MTR 依次 drop inode、删除 page3 cache/free owners，并在仍有 free successor 时清其 prev。
     * MTR commit 后才删除运行期 owner；声明物理修改后任一异常都包装为 fatal 并保留 drain gate。
     */
    private void drainBatch(SpaceId spaceId, UndoSegmentReuseDirectory.DrainLease drain,
                            UndoSegmentReuseDirectory.DrainBatch batch) {
        List<UndoSegmentDropPlan> plans = new ArrayList<>(batch.size());
        List<CachedUndoSegmentRef> cachedSegments = new ArrayList<>();
        cachedSegments.addAll(batch.insertTopFirst());
        cachedSegments.addAll(batch.updateTopFirst());
        for (CachedUndoSegmentRef cached : cachedSegments) {
            validateCachedHeader(cached);
            UndoSegmentDropPlan plan = inspectDropPlan(cached);
            if (plan.usedPageCount() != 1L || plan.fragmentPageCount() != 1L || plan.extentCount() != 0L) {
                throw new UndoTablespaceTruncationException("cached undo owner is not a single fragment segment: "
                        + cached.handle().firstPageId() + ", plan=" + plan);
            }
            plans.add(plan);
        }
        for (FreeUndoSegmentRef free : batch.freeHeadFirst()) {
            validateFreeHeader(free);
            UndoSegmentDropPlan plan = inspectDropPlan(free.handle());
            requireSingleFragment(plan, free.handle().firstPageId(), "free");
            plans.add(plan);
        }

        RollbackSegmentHeaderSnapshot persistent = readHeader(spaceId);
        UndoSegmentReuseDirectory.ReuseSnapshot runtime = reuseDirectory.snapshot();
        RollbackSegmentFreeListBase freeBase = persistent.freeListBase();
        Optional<FreeUndoSegmentRef> remainingFreeHead = batch.freeHeadFirst().size() == batch.expectedFreeCount()
                ? Optional.empty() : Optional.of(runtime.free().get(batch.freeHeadFirst().size()));
        requireBatchFreeBase(batch, runtime, freeBase);

        RedoBudgetWorkload workload = UndoRedoBudgetEstimator.finalization(plans, false);
        if (remainingFreeHead.isPresent() && !batch.freeHeadFirst().isEmpty()) {
            workload = workload.plus(RedoBudgetWorkload.pageImages(1L));
        }
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.UNDO_FINALIZATION,
                workload));
        try {
            drain.physicalMutationStarted(batch);
            for (CachedUndoSegmentRef cached : cachedSegments) {
                allocator.dropUndoSegment(mtr, cached.handle());
            }
            for (FreeUndoSegmentRef free : batch.freeHeadFirst()) {
                allocator.dropUndoSegment(mtr, free.handle());
            }
            List<RollbackSegmentHeaderRepository.CacheTopRemoval> cacheRemovals = removals(batch);
            if (!cacheRemovals.isEmpty()) {
                headerRepository.removeCachedTops(mtr, spaceId, cacheRemovals);
            }
            if (!batch.freeHeadFirst().isEmpty()) {
                headerRepository.removeFreeHeads(mtr, spaceId, freeBase,
                        batch.freeHeadFirst().stream().map(item -> item.handle().firstPageId()).toList(),
                        remainingFreeHead.map(item -> item.handle().firstPageId()));
                if (remainingFreeHead.isPresent()) {
                    undoAccess.relinkFreeHeadAfterDrain(mtr, remainingFreeHead.orElseThrow(),
                            batch.freeHeadFirst().getLast().handle().firstPageId());
                }
            }
            mtrManager.commit(mtr);
            drain.completeBatch(batch);
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr, failure);
            throw new UndoReusableSegmentDrainException(
                    "reusable undo drain failed after physical mutation began: space=" + spaceId.value(), failure);
        }
    }

    /**
     * 校验 {@code validateFreeHeader} 涉及的存储引擎稳定 API结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param expected 参与 {@code validateFreeHeader} 的稳定领域标识 {@code FreeUndoSegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws UndoTablespaceTruncationException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private void validateFreeHeader(FreeUndoSegmentRef expected) {
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            var current = undoAccess.inspectFree(mtr, expected.handle().firstPageId());
            if (!current.segment().equals(expected)) {
                throw new UndoTablespaceTruncationException("free undo handle changed before truncate drain: expected="
                        + expected + ", current=" + current.segment());
            }
            mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr, failure);
            throw failure;
        }
    }

    private static void requireSingleFragment(UndoSegmentDropPlan plan, PageId firstPage, String ownerKind) {
        if (plan.usedPageCount() != 1L || plan.fragmentPageCount() != 1L || plan.extentCount() != 0L) {
            throw new UndoTablespaceTruncationException(ownerKind
                    + " undo owner is not a single fragment segment: " + firstPage + ", plan=" + plan);
        }
    }

    /**
     * 校验 {@code requireBatchFreeBase} 涉及的存储引擎稳定 API结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param batch 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param runtime 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param persistent 参与 {@code requireBatchFreeBase} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws UndoTablespaceTruncationException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private static void requireBatchFreeBase(UndoSegmentReuseDirectory.DrainBatch batch,
                                             UndoSegmentReuseDirectory.ReuseSnapshot runtime,
                                             RollbackSegmentFreeListBase persistent) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        List<PageId> runtimePages = runtime.free().stream().map(item -> item.handle().firstPageId()).toList();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        Optional<PageId> head = runtimePages.isEmpty() ? Optional.empty() : Optional.of(runtimePages.getFirst());
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        Optional<PageId> tail = runtimePages.isEmpty() ? Optional.empty() : Optional.of(runtimePages.getLast());
        List<PageId> batchPages = batch.freeHeadFirst().stream()
                .map(item -> item.handle().firstPageId()).toList();
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        if (runtimePages.size() != batch.expectedFreeCount() || persistent.length() != runtimePages.size()
                || !persistent.headPageId().equals(head) || !persistent.tailPageId().equals(tail)
                || !runtimePages.subList(0, batchPages.size()).equals(batchPages)) {
            throw new UndoTablespaceTruncationException(
                    "runtime/persistent free FIFO changed before truncate batch: runtime="
                            + runtimePages + ", persistent=" + persistent + ", batch=" + batchPages);
        }
    }

    private void validateCachedHeader(CachedUndoSegmentRef expected) {
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            CachedUndoSegmentRef current = undoAccess.inspectCached(
                    mtr, expected.handle().firstPageId(), expected.kind());
            if (!current.equals(expected)) {
                throw new UndoTablespaceTruncationException("cached undo handle changed before truncate drain: expected="
                        + expected + ", current=" + current);
            }
            mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr, failure);
            throw failure;
        }
    }

    private UndoSegmentDropPlan inspectDropPlan(CachedUndoSegmentRef cached) {
        return inspectDropPlan(cached.handle());
    }

    private UndoSegmentDropPlan inspectDropPlan(cn.zhangyis.db.storage.undo.UndoSegmentHandle handle) {
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            UndoSegmentDropPlan plan = allocator.inspectDropPlan(mtr, handle);
            mtrManager.commit(mtr);
            return plan;
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr, failure);
            throw failure;
        }
    }

    /**
     * 定位并读取存储引擎稳定 API领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code readHeader} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private RollbackSegmentHeaderSnapshot readHeader(SpaceId spaceId) {
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            RollbackSegmentHeaderSnapshot snapshot = headerRepository.read(
                    mtr, spaceId, rollbackSegmentId, slotCapacity, cacheCapacityPerKind);
            mtrManager.commit(mtr);
            return snapshot;
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr, failure);
            throw failure;
        }
    }

    private void requireRuntimeStacksMatch(RollbackSegmentHeaderSnapshot persistent) {
        UndoSegmentReuseDirectory.ReuseSnapshot runtime = reuseDirectory.snapshot();
        List<PageId> runtimeInsert = firstPages(runtime.insert());
        List<PageId> runtimeUpdate = firstPages(runtime.update());
        if (!runtimeInsert.equals(persistent.cachedInsertSegments())
                || !runtimeUpdate.equals(persistent.cachedUpdateSegments())) {
            throw new UndoTablespaceTruncationException("runtime/persistent undo cache stacks differ: runtime="
                    + runtimeInsert + "/" + runtimeUpdate + ", persistent="
                    + persistent.cachedInsertSegments() + "/" + persistent.cachedUpdateSegments());
        }
        RollbackSegmentFreeListBase free = persistent.freeListBase();
        List<PageId> runtimeFree = runtime.free().stream().map(item -> item.handle().firstPageId()).toList();
        Optional<PageId> runtimeHead = runtimeFree.isEmpty() ? Optional.empty() : Optional.of(runtimeFree.getFirst());
        Optional<PageId> runtimeTail = runtimeFree.isEmpty() ? Optional.empty() : Optional.of(runtimeFree.getLast());
        if (free.length() != runtimeFree.size() || !free.headPageId().equals(runtimeHead)
                || !free.tailPageId().equals(runtimeTail)) {
            throw new UndoTablespaceTruncationException("runtime/persistent undo free FIFO differs: runtime="
                    + runtimeFree + ", persistent=" + free);
        }
    }

    private static List<RollbackSegmentHeaderRepository.CacheTopRemoval> removals(
            UndoSegmentReuseDirectory.DrainBatch batch) {
        List<RollbackSegmentHeaderRepository.CacheTopRemoval> removals = new ArrayList<>(2);
        if (!batch.insertTopFirst().isEmpty()) {
            removals.add(new RollbackSegmentHeaderRepository.CacheTopRemoval(
                    UndoLogKind.INSERT, batch.expectedInsertCount(), firstPages(batch.insertTopFirst())));
        }
        if (!batch.updateTopFirst().isEmpty()) {
            removals.add(new RollbackSegmentHeaderRepository.CacheTopRemoval(
                    UndoLogKind.UPDATE, batch.expectedUpdateCount(), firstPages(batch.updateTopFirst())));
        }
        return List.copyOf(removals);
    }

    private static List<PageId> firstPages(List<CachedUndoSegmentRef> cached) {
        return cached.stream().map(item -> item.handle().firstPageId()).toList();
    }

    private static void requireSpace(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("undo truncate space id must not be null");
        }
    }

    /**
     * 校验当前状态后推进存储引擎稳定 API状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param original 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private void rollbackIfActive(MiniTransaction mtr, RuntimeException original) {
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            return;
        }
        try {
            mtrManager.rollbackUncommitted(mtr);
        } catch (RuntimeException cleanupFailure) {
            original.addSuppressed(cleanupFailure);
        }
    }
}
