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

    private UndoFinalizationTestSupport() {
    }

    /** 构造共享 header repository 与 finalizer；调用方建 undo tablespace 后必须执行 {@link Components#format}。 */
    static Components create(MiniTransactionManager manager, BufferPool pool, PageSize pageSize,
                             UndoLogSegmentAccess access, UndoSpaceAllocator allocator,
                             RollbackSegmentSlotManager slots) {
        return create(manager, pool, pageSize, access, allocator, slots, UndoFinalizationFaultInjector.none());
    }

    /** 构造带 commit 后 crash hook 的测试 finalizer；hook 不进入生产构造路径。 */
    static Components create(MiniTransactionManager manager, BufferPool pool, PageSize pageSize,
                             UndoLogSegmentAccess access, UndoSpaceAllocator allocator,
                             RollbackSegmentSlotManager slots, UndoFinalizationFaultInjector faultInjector) {
        RollbackSegmentHeaderRepository header = new RollbackSegmentHeaderRepository(pool, pageSize);
        return new Components(header,
                new UndoSegmentFinalizer(manager, access, allocator, header, slots, faultInjector), slots);
    }

    /** 测试组合件；format 与 tablespace create 使用同一 boot MTR。 */
    record Components(RollbackSegmentHeaderRepository header, UndoSegmentFinalizer finalizer,
                      RollbackSegmentSlotManager slots) {

        void format(MiniTransaction bootMtr, SpaceId undoSpace) {
            header.format(bootMtr, undoSpace, slots.rollbackSegmentId(), slots.slotCapacity());
        }

        UndoLogManager manager(UndoLogSegmentAccess access, SpaceId undoSpace, HistoryList history,
                               MiniTransactionManager miniTransactionManager) {
            return new UndoLogManager(access, slots, undoSpace, history, header, miniTransactionManager, finalizer);
        }
    }
}
