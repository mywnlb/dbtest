package cn.zhangyis.db.storage.redo;

/**
 * 后台 redo flusher worker 生命周期状态。只描述刷盘线程，不代表 redo durable LSN 本身。
 */
public enum RedoFlushWorkerState {
    /** worker 尚未启动。 */
    NEW,
    /** worker 已启动，当前无待刷或刚完成一轮，等待下一个周期/请求。 */
    IDLE,
    /** worker 正在执行一轮 flush。 */
    RUNNING,
    /** 调用方已请求停止，worker 正在退出。 */
    STOPPING,
    /** worker 已完全停止，不再接受请求。 */
    STOPPED,
    /** worker 执行 flush 时遇到领域异常并停止。 */
    FAILED
}
