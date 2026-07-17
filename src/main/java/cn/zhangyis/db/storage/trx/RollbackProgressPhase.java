package cn.zhangyis.db.storage.trx;

/**
 * 完整回滚逐条进度的可测试 crash 边界。枚举只在 {@code storage.trx} 包内可见，避免把故障注入协议暴露成
 * storage facade 的稳定 API；生产构造始终使用 no-op injector。
 */
enum RollbackProgressPhase {

    /** 单个二级索引物理 inverse 的短 MTR 已提交；其它二级树、聚簇树与 logical-head marker 可能尚未处理。 */
    AFTER_SECONDARY_INVERSE_COMMIT,

    /** 当前 undo 的聚簇 inverse 已提交，但 first-page logical-head marker 尚未开始。 */
    AFTER_INVERSE_COMMIT,

    /** logical-head marker 已提交；live rollback 的 UndoContext 也已同步到新的持久边界。 */
    AFTER_PROGRESS_COMMIT
}
