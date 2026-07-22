package cn.zhangyis.db.storage.trx;

/** purge worker pool 生命周期；FAILED/STOPPED 均不再接纳批次。 */
enum PurgeWorkerPoolState {
    /** 接纳唯一批次，worker 可以提交新的记录级任务。 */
    RUNNING,
    /** 已拒绝新批次并取消排队 stage，正在等待记录内任务到达稳定边界。 */
    STOPPING,
    /** 所有平台线程已退出，生命周期正常收口。 */
    STOPPED,
    /** 批次超时或调度基础设施失败；拒绝重启，运行任务仍需到稳定边界后退出。 */
    FAILED
}
