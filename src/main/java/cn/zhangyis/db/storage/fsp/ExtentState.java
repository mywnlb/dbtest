package cn.zhangyis.db.storage.fsp;

/**
 * Extent 分配状态（设计 §5.4）。ordinal 落盘到 XDES，顺序不可改（FspEnumTest 钉死）。FREE 必须为 ordinal 0，
 * 使零初始化的 XDES entry 解码为 FREE。
 */
public enum ExtentState {
    /** 完全空闲。 */
    FREE,
    /** 可按 fragment page 分配。 */
    FREE_FRAG,
    /** fragment page 已满。 */
    FULL_FRAG,
    /** 完整属于某 segment。 */
    FSEG,
    /** 属于某 segment 但按 fragment 管理。 */
    FSEG_FRAG
}
