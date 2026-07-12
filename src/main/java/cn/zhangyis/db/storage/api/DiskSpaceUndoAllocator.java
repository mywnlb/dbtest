package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservation;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservationKind;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;
import cn.zhangyis.db.storage.undo.UndoSpaceReservation;

/**
 * {@link UndoSpaceAllocator} 的磁盘空间适配器。它位于 storage.api 层，负责把 undo 自有定位信息转换为
 * {@link SegmentRef} 并调用 {@link DiskSpaceManager}；因此依赖方向是 api → undo 端口，undo 模块不需要知道
 * DiskSpaceManager 或 SegmentRef 的存在。
 *
 * <p>适配器无跨调用可变状态，线程安全性由注入的 DiskSpaceManager/MTR 边界负责。
 */
public final class DiskSpaceUndoAllocator implements UndoSpaceAllocator {

    /**
     * 底层 FSP/Disk 门面。字段只在构造后读取，适配器不缓存 segment 或页状态，避免与 MTR 内权威状态分叉。
     */
    private final DiskSpaceManager diskSpaceManager;

    public DiskSpaceUndoAllocator(DiskSpaceManager diskSpaceManager) {
        if (diskSpaceManager == null) {
            throw new DatabaseValidationException("disk space manager must not be null");
        }
        this.diskSpaceManager = diskSpaceManager;
    }

    /**
     * 建 UNDO segment 并分配首页。DiskSpaceManager 会先把页初始化为 ALLOCATED；undo 上层随后在同一 MTR
     * 重初始化为 UNDO first page，redo 顺序保证最终页类型可恢复为 UNDO。
     */
    @Override
    public UndoSegmentHandle createUndoSegment(MiniTransaction mtr, SpaceId undoSpace) {
        SegmentRef ref = diskSpaceManager.createSegment(mtr, undoSpace, SegmentPurpose.UNDO);
        PageId first = diskSpaceManager.allocatePage(mtr, ref);
        return new UndoSegmentHandle(undoSpace, ref.inodeSlot(), ref.segmentId(), first, first);
    }

    /**
     * 为 undo 页链增长预留容量。适配器把 undo 自有端口映射为 DiskSpaceManager 的 UNDO reservation，
     * 使真正的 {@link #allocatePage} 在同一 MTR 中消费页额度；返回值只暴露 undo 端口，避免 undo 模块依赖 api/fsp 类型。
     */
    @Override
    public UndoSpaceReservation reserveGrowPages(MiniTransaction mtr, SpaceId undoSpace, long pages) {
        SpaceReservation reservation = diskSpaceManager.reserveSpace(mtr, undoSpace, SpaceReservationKind.UNDO,
                pages, 0L);
        return reservation::close;
    }

    /**
     * 按 handle 中落盘的 inodeSlot/segmentId 重建 SegmentRef，并在同一 segment 内续分配一页。
     */
    @Override
    public PageId allocatePage(MiniTransaction mtr, SpaceId undoSpace, int inodeSlot, SegmentId segmentId) {
        SegmentRef ref = new SegmentRef(undoSpace, inodeSlot, segmentId);
        return diskSpaceManager.allocatePage(mtr, ref);
    }

    /** 把 undo handle 转为 SegmentRef，并把通用 FSP drop plan 映射回 undo 自有值对象。 */
    @Override
    public UndoSegmentDropPlan inspectDropPlan(MiniTransaction mtr, UndoSegmentHandle handle) {
        if (handle == null) {
            throw new DatabaseValidationException("undo segment drop plan handle must not be null");
        }
        SegmentDropPlan plan = diskSpaceManager.inspectDropSegmentPlan(mtr,
                new SegmentRef(handle.spaceId(), handle.inodeSlot(), handle.segmentId()));
        return new UndoSegmentDropPlan(
                plan.fragmentPageCount(), plan.extentCount(), plan.usedPageCount());
    }

    /**
     * 按 handle 的 inodeSlot/segmentId 重建 SegmentRef 并调用 {@link DiskSpaceManager#dropSegment} 物理回收整段。
     */
    @Override
    public void dropUndoSegment(MiniTransaction mtr, UndoSegmentHandle handle) {
        if (handle == null) {
            throw new DatabaseValidationException("undo segment handle must not be null");
        }
        SegmentRef ref = new SegmentRef(handle.spaceId(), handle.inodeSlot(), handle.segmentId());
        diskSpaceManager.dropSegment(mtr, ref);
    }
}
