package cn.zhangyis.db.storage.recovery;

/**
 * recovery gate/service 状态。普通用户流量只有在 OPEN 后才能进入存储引擎。
 */
public enum RecoveryState {
    /** 初始或 fail 后关闭普通流量。 */
    CLOSED,
    /** recovery 主线程正在执行阶段链。 */
    RECOVERING,
    /** 所有 R2 必需阶段完成，普通流量可开放。 */
    OPEN,
    /** 恢复失败，gate 保持关闭，调用方必须报告错误或终止启动。 */
    FAILED
}
