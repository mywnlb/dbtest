package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.trx.UndoRedoBudgetEstimator;
import cn.zhangyis.db.storage.trx.UndoSegmentCacheDirectory;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;

import java.util.ArrayList;
import java.util.List;

/**
 * UNDO truncate 与 cached segment 目录之间的协作者。调用方必须已持目标表空间 lifecycle X lease；本类只用
 * 非阻塞 cache drain gate，并把每批最多八个 segment 的 FSP drop 与 page3 top removal 放入同一 MTR。
 *
 * <p>锁序是：短只读 undo 首页校验并释放 → 短只读 FSP inode 校验并释放 → 写 MTR 依次修改 page0/page2/page3。
 * 任何 active slot 都在首个 cache drop 之前拒绝，避免 truncate 回收仍属于事务的 segment。
 */
public final class UndoCachedSegmentTruncationCoordinator {

    private static final int MAX_SEGMENTS_PER_MTR = 8;

    private final MiniTransactionManager mtrManager;
    private final UndoLogSegmentAccess undoAccess;
    private final UndoSpaceAllocator allocator;
    private final RollbackSegmentHeaderRepository headerRepository;
    private final RollbackSegmentId rollbackSegmentId;
    private final int slotCapacity;
    private final int cacheCapacityPerKind;
    private final UndoSegmentCacheDirectory cacheDirectory;

    /**
     * 创建 cache drain 协作者；所有依赖必须来自同一 StorageEngine 组合根，容量必须与 page3 和运行期目录一致。
     *
     * @param mtrManager drain 读写 MTR 来源。
     * @param undoAccess cached 首页格式校验入口。
     * @param allocator FSP inode 检查与 segment drop 入口。
     * @param headerRepository page3 active/cache owner 仓储。
     * @param rollbackSegmentId 当前唯一 rollback segment id。
     * @param slotCapacity page3 active slot 持久容量。
     * @param cacheCapacityPerKind page3 每类 cache 持久容量。
     * @param cacheDirectory 与 page3 对应的运行期缓存投影。
     */
    public UndoCachedSegmentTruncationCoordinator(
            MiniTransactionManager mtrManager,
            UndoLogSegmentAccess undoAccess,
            UndoSpaceAllocator allocator,
            RollbackSegmentHeaderRepository headerRepository,
            RollbackSegmentId rollbackSegmentId,
            int slotCapacity,
            int cacheCapacityPerKind,
            UndoSegmentCacheDirectory cacheDirectory) {
        if (mtrManager == null || undoAccess == null || allocator == null || headerRepository == null
                || rollbackSegmentId == null || cacheDirectory == null) {
            throw new DatabaseValidationException("undo cached-segment truncation dependencies must not be null");
        }
        if (slotCapacity <= 0 || cacheCapacityPerKind < 0
                || cacheDirectory.capacityPerKind() != cacheCapacityPerKind) {
            throw new DatabaseValidationException("undo cached-segment truncation capacities are invalid");
        }
        this.mtrManager = mtrManager;
        this.undoAccess = undoAccess;
        this.allocator = allocator;
        this.headerRepository = headerRepository;
        this.rollbackSegmentId = rollbackSegmentId;
        this.slotCapacity = slotCapacity;
        this.cacheCapacityPerKind = cacheCapacityPerKind;
        this.cacheDirectory = cacheDirectory;
    }

    /**
     * stable truncate 在 marker 前执行：非阻塞封锁运行期 cache，先拒绝 persistent history/active slots，
     * 再逐批 drop cached owners。
     * gate 忙表示某 finalizer/reuser 已跨出短锁，调用方必须释放 lifecycle X 后重试，不能在 X 下等待。
     */
    public void drainStableSpace(SpaceId spaceId) {
        requireSpace(spaceId);
        UndoSegmentCacheDirectory.DrainLease drain = cacheDirectory.tryBeginDrain().orElseThrow(() ->
                new UndoTablespaceTruncationException(
                        "undo cache transition is in progress; release lifecycle X lease and retry truncate"));
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
     * cache directory，因此只读取 page3；任何 history/active/cache owner 都说明 marker 顺序被破坏，必须 fail-closed。
     */
    public void requirePersistentEmpty(SpaceId spaceId) {
        RollbackSegmentHeaderSnapshot snapshot = readHeader(spaceId);
        if (!snapshot.occupiedSlots().isEmpty()
                || !snapshot.cachedInsertSegments().isEmpty()
                || !snapshot.cachedUpdateSegments().isEmpty()
                || snapshot.historyBase().length() != 0L) {
            throw new UndoTablespaceNotEmptyException("TRUNCATING UNDO space still has rollback segment owners: space="
                    + spaceId.value() + ", active=" + snapshot.occupiedSlots().size()
                    + ", insertCache=" + snapshot.cachedInsertSegments().size()
                    + ", updateCache=" + snapshot.cachedUpdateSegments().size()
                    + ", history=" + snapshot.historyBase().length());
        }
    }

    /** 物理截断后在 page0/page2 重建同一 MTR 中重建 page3 v3 空目录。 */
    public void rebuildHeader(MiniTransaction mtr, SpaceId spaceId) {
        headerRepository.format(mtr, spaceId, rollbackSegmentId, slotCapacity, cacheCapacityPerKind);
    }

    private void drainBatch(SpaceId spaceId, UndoSegmentCacheDirectory.DrainLease drain,
                            UndoSegmentCacheDirectory.DrainBatch batch) {
        List<UndoSegmentDropPlan> plans = new ArrayList<>(batch.segments().size());
        for (CachedUndoSegmentRef cached : batch.segments()) {
            validateCachedHeader(cached);
            UndoSegmentDropPlan plan = inspectDropPlan(cached);
            if (plan.usedPageCount() != 1L || plan.fragmentPageCount() != 1L || plan.extentCount() != 0L) {
                throw new UndoTablespaceTruncationException("cached undo owner is not a single fragment segment: "
                        + cached.handle().firstPageId() + ", plan=" + plan);
            }
            plans.add(plan);
        }

        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.UNDO_FINALIZATION,
                UndoRedoBudgetEstimator.finalization(plans, false)));
        try {
            drain.physicalMutationStarted(batch);
            for (CachedUndoSegmentRef cached : batch.segments()) {
                allocator.dropUndoSegment(mtr, cached.handle());
            }
            headerRepository.removeCachedTops(mtr, spaceId, removals(batch));
            mtrManager.commit(mtr);
            drain.completeBatch(batch);
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr, failure);
            throw new UndoCachedSegmentDrainException(
                    "cached undo drain failed after physical mutation began: space=" + spaceId.value(), failure);
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
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            UndoSegmentDropPlan plan = allocator.inspectDropPlan(mtr, cached.handle());
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
        UndoSegmentCacheDirectory.CacheSnapshot runtime = cacheDirectory.snapshot();
        List<PageId> runtimeInsert = firstPages(runtime.insert());
        List<PageId> runtimeUpdate = firstPages(runtime.update());
        if (!runtimeInsert.equals(persistent.cachedInsertSegments())
                || !runtimeUpdate.equals(persistent.cachedUpdateSegments())) {
            throw new UndoTablespaceTruncationException("runtime/persistent undo cache stacks differ: runtime="
                    + runtimeInsert + "/" + runtimeUpdate + ", persistent="
                    + persistent.cachedInsertSegments() + "/" + persistent.cachedUpdateSegments());
        }
    }

    private static List<RollbackSegmentHeaderRepository.CacheTopRemoval> removals(
            UndoSegmentCacheDirectory.DrainBatch batch) {
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
