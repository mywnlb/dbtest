package cn.zhangyis.db.storage.fsp;

/**
 * Segment 用途（设计 §5.5）。ordinal 落盘到 INODE，顺序不可改（FspEnumTest 钉死）。
 */
public enum SegmentPurpose {
    /** 叶子页 segment。 */
    INDEX_LEAF,
    /** 非叶子页 segment。 */
    INDEX_NON_LEAF,
    /** 大字段溢出页 segment。 */
    LOB,
    /** undo segment。 */
    UNDO,
    /** 系统 segment。 */
    SYSTEM
}
