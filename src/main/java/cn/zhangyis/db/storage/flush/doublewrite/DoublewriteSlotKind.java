package cn.zhangyis.db.storage.flush.doublewrite;

/**
 * doublewrite slot 的持久化内容类型。恢复扫描必须用该类型区分“可修复副本”和“仅可诊断元数据”。
 */
public enum DoublewriteSlotKind {
    /** slot payload 保存完整页镜像，恢复期可用于修复 torn/corrupt data page。 */
    FULL_COPY,
    /** slot payload 只保存页定位和校验元数据，恢复期不能把它写回 data file。 */
    DETECT_ONLY_METADATA
}
