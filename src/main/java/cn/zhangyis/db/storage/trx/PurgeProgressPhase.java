package cn.zhangyis.db.storage.trx;

/** Purge 单索引物理 MTR 已提交、history 尚未 finalization 的稳定故障边界。 */
enum PurgeProgressPhase {
    /** 一个 delete-marked secondary entry 的删除 MTR 已 durable/dirty-published。 */
    AFTER_SECONDARY_COMMIT,
    /** DELETE_MARK 对应 clustered 记录的物理删除 MTR 已 durable/dirty-published。 */
    AFTER_CLUSTERED_COMMIT
}
