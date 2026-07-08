package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;

import java.util.Optional;

/**
 * Extent 分配策略输入（disk-manager §7.3）。该 record 位于 FSP 内部，避免底层依赖
 * {@code storage.api.PageAllocationHint}；DiskSpaceManager/SegmentPageAllocator 负责把外部页分配 hint
 * 转换成这里的完整上下文。
 *
 * @param segmentPurpose   segment 用途；leaf segment 才会消费顺序增长批量 extent。
 * @param hintPageNo       调用方提供的邻近页号；方向为 UP/DOWN 时用于定位候选 extent。
 * @param direction        分配方向；NO_DIRECTION 保持链头分配。
 * @param pagesNeeded      本次上层操作预计最多需要的新页数，用于把批量 extent 下限抬高到足够覆盖请求。
 * @param ownedExtentCount 当前 segment 已拥有的完整 extent 数（三条 SEG 链之和），表达 segment 是否已经“大”。
 * @param usedPageCount    当前 segment 已使用页数；首版仅保留给后续 reserveFactor/增长率策略，当前默认策略不直接使用。
 * @param tablespaceSize   page0 中的当前表空间大小；首版用于诊断和后续策略扩展，不直接改变取数。
 * @param reserveFactor    预留因子；本片固定传 1.0，后续可用于 IO capacity/空间压力调节。
 * @param pageSize         实例页大小；策略必须用它换算 pagesNeeded 到 extent 数，不能写死 64。
 */
public record ExtentAllocationRequest(
        SegmentPurpose segmentPurpose,
        Optional<PageNo> hintPageNo,
        ExtentAllocationDirection direction,
        long pagesNeeded,
        long ownedExtentCount,
        long usedPageCount,
        PageNo tablespaceSize,
        double reserveFactor,
        PageSize pageSize) {

    public ExtentAllocationRequest {
        if (segmentPurpose == null || hintPageNo == null || direction == null
                || tablespaceSize == null || pageSize == null) {
            throw new DatabaseValidationException("extent allocation request fields must not be null");
        }
        if (pagesNeeded <= 0L) {
            throw new DatabaseValidationException("pagesNeeded must be positive: " + pagesNeeded);
        }
        if (direction != ExtentAllocationDirection.NO_DIRECTION && hintPageNo.isEmpty()) {
            throw new DatabaseValidationException("directional extent allocation request requires a hint page");
        }
        if (ownedExtentCount < 0L || usedPageCount < 0L) {
            throw new DatabaseValidationException("extent allocation counters must be non-negative");
        }
        if (reserveFactor <= 0.0d || Double.isNaN(reserveFactor) || Double.isInfinite(reserveFactor)) {
            throw new DatabaseValidationException("reserveFactor must be a finite positive number: " + reserveFactor);
        }
    }
}
