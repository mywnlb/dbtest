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
     * @param mtrManager drain 读写 MTR 来源。
     * @param undoAccess cache/free 首页格式校验入口。
     * @param allocator FSP inode 检查与 segment drop 入口。
     * @param headerRepository page3 active/cache/free owner 仓储。
     * @param rollbackSegmentId 当前唯一 rollback segment id。
     * @param slotCapacity page3 active slot 持久容量。
     * @param cacheCapacityPerKind page3 每类 cache 持久容量。
     * @param reuseDirectory 与 page3 对应的运行期可复用目录投影。
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
        if (mtrManager == null || undoAccess == null || allocator == null || headerRepository == null
                || rollbackSegmentId == null || reuseDirectory == null) {
            throw new DatabaseValidationException("undo reusable-segment truncation dependencies must not be null");
        }
        if (slotCapacity <= 0 || cacheCapacityPerKind < 0
                || reuseDirectory.capacityPerKind() != cacheCapacityPerKind) {
            throw new DatabaseValidationException("undo reusable-segment truncation capacities are invalid");
        }
        this.mtrManager = mtrManager;
        this.undoAccess = undoAccess;
        this.allocator = allocator;
        this.headerRepository = headerRepository;
        this.rollbackSegmentId = rollbackSegmentId;
        this.slotCapacity = slotCapacity;
        this.cacheCapacityPerKind = cacheCapacityPerKind;
        this.reuseDirectory = reuseDirectory;
    }

    /**
     * stable truncate 在 marker 前执行：非阻塞封锁运行期 reuse directory，先拒绝 persistent history/active slots，
     * 再按 INSERT cache、UPDATE cache、free FIFO 顺序逐批 drop owners。
     * gate 忙表示某 finalizer/reuser 已跨出短锁，调用方必须释放 lifecycle X 后重试，不能在 X 下等待。
     */
    public void drainStableSpace(SpaceId spaceId) {
        requireSpace(spaceId);
        UndoSegmentReuseDirectory.DrainLease drain = reuseDirectory.tryBeginDrain().orElseThrow(() ->
                new UndoTablespaceTruncationException(
                        "undo reuse transition is in progress; release lifecycle X lease and retry truncate"));
        try (drain) {
            RollbackSegmentHeaderSnapshot snapshot = readHeader(spaceId);
            if (snapshot.historyBase().length() != 0L) {
                throw new UndoTablespaceNotEmptyException("UNDO rollback segment history is not empty: space="
                        + spaceId.value() + ", length=" + snapshot.historyBase().length());
            }
            if (!snapshot.occupiedSlots().isEmpty()) {
                throw new UndoTablespaceNotEmptyException("UNDO rollback segment still owns active slots: space="
                        + spaceId.value() + ", count=" + snapshot.occupiedSlots().size());
            }
            requireRuntimeStacksMatch(snapshot);

            while (true) {
                var optionalBatch = drain.nextBatch(MAX_SEGMENTS_PER_MTR);
                if (optionalBatch.isEmpty()) {
                    break;
                }
                drainBatch(spaceId, drain, optionalBatch.orElseThrow());
            }
            requirePersistentEmpty(spaceId);
            drain.finish();
        }
    }

    /**
     * durable TRUNCATING 恢复只能续作一个已经在 marker 前清空 owner 的空间。此时 transaction recovery 尚未重建
     * reuse directory，因此只读取 page3；任何 history/active/cache/free owner 都说明 marker 顺序被破坏，必须 fail-closed。
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

    /** 物理截断后在 page0/page2 重建同一 MTR 中重建 page3 v4 空目录。 */
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

    private static void requireBatchFreeBase(UndoSegmentReuseDirectory.DrainBatch batch,
                                             UndoSegmentReuseDirectory.ReuseSnapshot runtime,
                                             RollbackSegmentFreeListBase persistent) {
        List<PageId> runtimePages = runtime.free().stream().map(item -> item.handle().firstPageId()).toList();
        Optional<PageId> head = runtimePages.isEmpty() ? Optional.empty() : Optional.of(runtimePages.getFirst());
        Optional<PageId> tail = runtimePages.isEmpty() ? Optional.empty() : Optional.of(runtimePages.getLast());
        List<PageId> batchPages = batch.freeHeadFirst().stream()
                .map(item -> item.handle().firstPageId()).toList();
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
