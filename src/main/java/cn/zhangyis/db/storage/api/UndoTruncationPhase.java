package cn.zhangyis.db.storage.api;

/** undo 截断显式故障边界；测试用它覆盖 marker、物理缩短、重建、最终发布间的 crash 续作。 */
public enum UndoTruncationPhase {
    AFTER_MARKER_DURABLE,
    AFTER_BUFFER_INVALIDATION,
    AFTER_PHYSICAL_TRUNCATE,
    AFTER_REBUILD_DURABLE,
    AFTER_FINAL_STATE_DURABLE
}
