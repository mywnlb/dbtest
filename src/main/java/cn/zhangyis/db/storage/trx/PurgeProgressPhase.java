package cn.zhangyis.db.storage.trx;

/** Purge 单索引或记录级物理 MTR 已提交、history 尚未 finalization 的稳定故障边界。 */
enum PurgeProgressPhase {
    /** 一个 delete-marked secondary entry 的删除 MTR 已 durable/dirty-published。 */
    AFTER_SECONDARY_COMMIT,
    /** DELETE_MARK 对应 clustered 记录的物理删除 MTR 已 durable/dirty-published。 */
    AFTER_CLUSTERED_COMMIT,
    /** 当前 undo record 的 LOB purge-old ownership 与 logical-head 前移已在同一 MTR 提交。 */
    AFTER_RECORD_PROGRESS_COMMIT
}
