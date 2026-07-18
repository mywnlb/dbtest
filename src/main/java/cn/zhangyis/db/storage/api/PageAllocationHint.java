package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;

import java.util.Optional;

/**
 * DiskSpaceManager 页分配 hint。它是 storage API 层给 FSP 的轻量提示：调用方可说明新页最好靠近哪个页号、
 * 向哪个物理方向增长，以及本次操作预计需要多少新页。hint 不绑定具体页号或 extent，真正归属仍由 FSP 元数据决定。
 *
 * @param direction 分配方向；NO_DIRECTION 表示保持旧链头分配语义。
 * @param hintPageNo 邻近页号；UP/DOWN 必须提供，NO_DIRECTION 必须为空。
 * @param pagesNeeded 本次操作预计最多需要的新页数，用于底层批量 extent 策略，必须为正。
 */
public record PageAllocationHint(Direction direction, Optional<PageNo> hintPageNo, long pagesNeeded) {

    public PageAllocationHint {
        if (direction == null || hintPageNo == null) {
            throw new DatabaseValidationException("page allocation hint direction/hint must not be null");
        }
        if (pagesNeeded <= 0L) {
            throw new DatabaseValidationException("page allocation pagesNeeded must be positive: " + pagesNeeded);
        }
        if (direction == Direction.NO_DIRECTION && hintPageNo.isPresent()) {
            throw new DatabaseValidationException("NO_DIRECTION allocation hint must not carry a page hint");
        }
        if (direction != Direction.NO_DIRECTION && hintPageNo.isEmpty()) {
            throw new DatabaseValidationException("directional allocation hint requires a page hint");
        }
    }

    /** 无方向 hint，完全保持旧分配行为。 */
    public static PageAllocationHint none() {
        return new PageAllocationHint(Direction.NO_DIRECTION, Optional.empty(), 1L);
    }

    /** 向更大页号方向增长。
     *
     * @param hintPageNo 参与 {@code up} 的稳定领域标识 {@code PageNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param pagesNeeded 参与 {@code up} 的上界或规格值 {@code pagesNeeded}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return {@code up} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    public static PageAllocationHint up(PageNo hintPageNo, long pagesNeeded) {
        requireHintPage(hintPageNo);
        return new PageAllocationHint(Direction.UP, Optional.of(hintPageNo), pagesNeeded);
    }

    /** 向更小页号方向增长。
     *
     * @param hintPageNo 参与 {@code down} 的稳定领域标识 {@code PageNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param pagesNeeded 参与 {@code down} 的上界或规格值 {@code pagesNeeded}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return {@code down} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    public static PageAllocationHint down(PageNo hintPageNo, long pagesNeeded) {
        requireHintPage(hintPageNo);
        return new PageAllocationHint(Direction.DOWN, Optional.of(hintPageNo), pagesNeeded);
    }

    private static void requireHintPage(PageNo hintPageNo) {
        if (hintPageNo == null) {
            throw new DatabaseValidationException("directional allocation hint page must not be null");
        }
    }

    /** API 层方向枚举；DiskSpaceManager 负责转换为 FSP 内部方向类型，避免 FSP 类型泄漏给调用方。 */
    public enum Direction {
        /** 无明确方向。 */
        NO_DIRECTION,
        /** 向更大页号方向。 */
        UP,
        /** 向更小页号方向。 */
        DOWN
    }
}
