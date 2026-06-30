package cn.zhangyis.db.storage.buf;

/**
 * {@link ReadAheadService} 后台 worker 的生命周期状态。
 */
public enum ReadAheadState {
    /** 已构造未启动。 */
    NEW,
    /** 已启动、队列空、worker 空闲等待。 */
    IDLE,
    /** 正在出队预取。 */
    RUNNING,
    /** 已请求停止、等待 worker 退出。 */
    STOPPING,
    /** worker 已退出。 */
    STOPPED
}
