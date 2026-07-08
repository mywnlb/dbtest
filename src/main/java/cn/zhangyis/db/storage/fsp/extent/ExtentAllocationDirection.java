package cn.zhangyis.db.storage.fsp.extent;

/**
 * Extent 分配方向（disk-manager §7.3）。方向只描述“从 hint 附近寻找空闲 extent”的偏好，
 * 不改变 XDES/FLST 的持久格式；普通无 hint 调用必须使用 {@link #NO_DIRECTION}，保持链头分配语义稳定。
 */
public enum ExtentAllocationDirection {
    /** 无明确顺序增长方向，按全局 free-list 默认顺序取 extent。 */
    NO_DIRECTION,
    /** 向更大页号方向增长，优先选择不小于 hint 所属 extent 的最近空闲 extent。 */
    UP,
    /** 向更小页号方向增长，优先选择不大于 hint 所属 extent 的最近空闲 extent。 */
    DOWN
}
