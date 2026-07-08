package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;

/**
 * 默认 extent 取数策略：只有 leaf segment 收到明确 UP/DOWN 顺序增长 hint 时才按“大 segment”批量获取 extent；
 * 普通 NO_DIRECTION、undo、non-leaf 等路径继续一次 1 个 extent，避免把随机 split 或元数据页分配误判为顺序增长。
 *
 * <p>批量规则对齐设计 §7.2/§7.3：最多 4 个 extent；owned extent 少于等于 1 时仍取 1；pagesNeeded 可把下限抬高到
 * 覆盖调用方声明的页需求，但仍被 4 封顶。
 */
public final class DefaultExtentAllocationPolicy implements ExtentAllocationPolicy {

    @Override
    public int extentsToAcquire(ExtentAllocationRequest request) {
        if (request == null) {
            throw new DatabaseValidationException("extent allocation request must not be null");
        }
        if (request.segmentPurpose() != SegmentPurpose.INDEX_LEAF
                || request.direction() == ExtentAllocationDirection.NO_DIRECTION) {
            return 1;
        }
        long byOwned = request.ownedExtentCount() <= 1L ? 1L : Math.min(4L, request.ownedExtentCount());
        long byPages = Math.floorDiv(request.pagesNeeded() + request.pageSize().pagesPerExtent() - 1L,
                request.pageSize().pagesPerExtent());
        return (int) Math.max(1L, Math.min(4L, Math.max(byOwned, byPages)));
    }
}
