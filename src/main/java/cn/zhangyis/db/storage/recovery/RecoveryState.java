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
    /**
     * 只读诊断模式完成：恢复输入已扫描并生成报告，但没有执行会修改 data file/redo 边界/undo 状态的阶段。
     * 普通写路径必须继续拒绝该状态，避免把诊断实例误当作可服务实例。
     */
    READ_ONLY,
    /** 恢复失败，gate 保持关闭，调用方必须报告错误或终止启动。 */
    FAILED
}
