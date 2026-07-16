package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;

/** 测试侧统一构造真实 page3 + atomic finalizer，避免 focused fixture 绕过生产 undo 生命周期。 */
final class UndoFinalizationTestSupport {

    private static final int CACHE_CAPACITY_PER_KIND = 8;

    private UndoFinalizationTestSupport() {
    }

    /** 构造共享 header repository 与 finalizer；调用方建 undo tablespace 后必须执行 {@link Components#format}。 */
    static Components create(MiniTransactionManager manager, BufferPool pool, PageSize pageSize,
                             UndoLogSegmentAccess access, UndoSpaceAllocator allocator,
                             RollbackSegmentSlotManager slots) {
        return create(manager, pool, pageSize, access, allocator, slots,
                UndoFinalizationFaultInjector.none(), CACHE_CAPACITY_PER_KIND);
    }

    /** 构造指定 cache 容量的组合件；0 用于仍需观察物理 drop 边界的旧回归测试。 */
    static Components create(MiniTransactionManager manager, BufferPool pool, PageSize pageSize,
                             UndoLogSegmentAccess access, UndoSpaceAllocator allocator,
                             RollbackSegmentSlotManager slots, int cacheCapacityPerKind) {
        return create(manager, pool, pageSize, access, allocator, slots,
                UndoFinalizationFaultInjector.none(), cacheCapacityPerKind);
    }

    /** 构造带 commit 后 crash hook 的测试 finalizer；hook 不进入生产构造路径。 */
    static Components create(MiniTransactionManager manager, BufferPool pool, PageSize pageSize,
                             UndoLogSegmentAccess access, UndoSpaceAllocator allocator,
                             RollbackSegmentSlotManager slots, UndoFinalizationFaultInjector faultInjector) {
        return create(manager, pool, pageSize, access, allocator, slots,
                faultInjector, CACHE_CAPACITY_PER_KIND);
    }

    /** 构造同时指定 crash hook 与 cache 容量的测试组合件。 */
    static Components create(MiniTransactionManager manager, BufferPool pool, PageSize pageSize,
                             UndoLogSegmentAccess access, UndoSpaceAllocator allocator,
                             RollbackSegmentSlotManager slots, UndoFinalizationFaultInjector faultInjector,
                             int cacheCapacityPerKind) {
        RollbackSegmentHeaderRepository header = new RollbackSegmentHeaderRepository(pool, pageSize);
        UndoSegmentReuseDirectory cache = new UndoSegmentReuseDirectory(cacheCapacityPerKind);
        return new Components(header,
                new UndoSegmentFinalizer(manager, access, allocator, header, slots, cache, faultInjector),
                slots, cache);
    }

    /** 测试组合件；format 与 tablespace create 使用同一 boot MTR。 */
    record Components(RollbackSegmentHeaderRepository header, UndoSegmentFinalizer finalizer,
                      RollbackSegmentSlotManager slots, UndoSegmentReuseDirectory cache) {

        void format(MiniTransaction bootMtr, SpaceId undoSpace) {
            header.format(bootMtr, undoSpace, slots.rollbackSegmentId(), slots.slotCapacity(),
                    cache.capacityPerKind());
        }

        UndoLogManager manager(UndoLogSegmentAccess access, SpaceId undoSpace, HistoryList history,
                               MiniTransactionManager ignoredMiniTransactionManager) {
            return new UndoLogManager(access, slots, undoSpace, history, header, finalizer, cache);
        }
    }
}
