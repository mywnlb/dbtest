package cn.zhangyis.db.storage.flush;

/**
 * 单页 flush 的结果状态。调用方用它区分是否已清脏、保留 dirty、跳过或失败。
 */
public enum FlushResultStatus {
    /** 页已成功写 doublewrite/data file，且 Buffer Pool 确认 snapshot 仍是当前版本，已清脏。 */
    CLEAN,
    /** 页已写盘，但 snapshot 后页面再次变脏，Buffer Pool 保留 dirty。 */
    KEPT_DIRTY,
    /** 选择到候选后发现页不存在、已 clean 或仍被 fixed，本轮跳过。 */
    SKIPPED_NOT_DIRTY,
    /** 页 LSN 对应 redo 尚未 durable，本轮不能写 data file。 */
    SKIPPED_REDO_NOT_DURABLE,
    /** doublewrite、data file write 或 force 失败；页必须保留 dirty。 */
    FAILED
}
