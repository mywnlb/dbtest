package cn.zhangyis.db.storage.flush.doublewrite;

/**
 * Doublewrite 保护模式。不同模式决定 flush 前写入 doublewrite 文件的是完整页副本，还是仅用于恢复期诊断的页元数据。
 */
public enum DoublewriteMode {
    /** 不写 doublewrite 副本，适合低可靠性测试路径。 */
    OFF,
    /** 只写页定位和校验元数据；恢复期可报告 torn/corrupt page，但不能用副本修复。 */
    DETECT_ONLY,
    /** 写完整页副本，恢复时可用它修复 torn/corrupt data page。 */
    DETECT_AND_RECOVER
}
