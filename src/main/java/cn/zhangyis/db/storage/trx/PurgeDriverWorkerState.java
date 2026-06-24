package cn.zhangyis.db.storage.trx;

/**
 * 后台 purge driver worker 生命周期状态（0.4）。只描述驱动线程，不代表 undo/history 物理状态。
 */
public enum PurgeDriverWorkerState {
    /** worker 尚未启动。 */
    NEW,
    /** worker 已启动，等待下一个周期 tick / 请求。 */
    IDLE,
    /** worker 正在执行一个 purge 批次。 */
    RUNNING,
    /** 调用方已请求停止，worker 正在退出。 */
    STOPPING,
    /** worker 已完全停止，不再接受请求。 */
    STOPPED,
    /** worker 执行 runBatch 时遇到领域异常并停止。 */
    FAILED
}
