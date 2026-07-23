package cn.zhangyis.db.storage.changebuffer;

/** Change Buffer 后台 merge worker 的显式生命周期。 */
public enum ChangeBufferWorkerState {
    /** 已构造但尚未启动。 */
    NEW,
    /** 正常选择目标并触发 demand-load merge。 */
    RUNNING,
    /** close 已请求，线程正在退出。 */
    STOPPING,
    /** 线程已正常退出。 */
    STOPPED,
    /** 后台 merge 遇到不可恢复运行时错误，实例应停止普通写流量。 */
    FAILED
}
