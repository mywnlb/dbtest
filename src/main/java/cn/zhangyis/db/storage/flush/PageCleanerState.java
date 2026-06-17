package cn.zhangyis.db.storage.flush;

/**
 * page cleaner worker 生命周期状态。该状态只描述后台刷脏线程，不代表 Buffer Pool frame 状态。
 */
public enum PageCleanerState {
    /** worker 尚未启动。 */
    NEW,
    /** worker 已启动，当前没有待处理请求。 */
    IDLE,
    /** worker 正在执行一轮 flush cycle。 */
    RUNNING,
    /** 调用方已请求停止，worker 正在退出。 */
    STOPPING,
    /** worker 已完全停止，不再接受新请求。 */
    STOPPED,
    /** worker 执行 flush cycle 时遇到可恢复领域异常并停止。 */
    FAILED
}
