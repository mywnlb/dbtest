package cn.zhangyis.db.storage.flush.doublewrite;

/**
 * Doublewrite 保护模式。F1 只实现 OFF 与可恢复 full-copy；DETECT_ONLY 留后续扩展，避免第一片过宽。
 */
public enum DoublewriteMode {
    /** 不写 doublewrite 副本，适合低可靠性测试路径。 */
    OFF,
    /** 写完整页副本，恢复时可用它修复 torn/corrupt data page。 */
    DETECT_AND_RECOVER
}
