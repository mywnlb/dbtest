package cn.zhangyis.db.storage.fsp.segment;

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
    SYSTEM,
    /** 通用Online ALTER的短期descriptor chain；追加在尾部以保持既有INODE ordinal。 */
    DDL_DESCRIPTOR
}
