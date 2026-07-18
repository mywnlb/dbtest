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

    /**
     * 按表空间、区与段分配并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code extentsToAcquire} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
