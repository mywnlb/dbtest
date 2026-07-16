package cn.zhangyis.db.engine;

/** DatabaseEngine 组合根生命周期。FAILED/CLOSED 不允许重新 open，避免复用半关闭资源。 */
public enum DatabaseEngineState {
    /** 尚未分配任何文件或后台资源。 */
    NEW,
    /** 正在执行 DD discovery、存储恢复和 DDL 收敛。 */
    OPENING,
    /** 所有恢复阶段成功，允许上层取得 facade。 */
    OPEN,
    /** 已拒绝新 Session/访问器，正在等待活动 Session cooperative close。 */
    CLOSING,
    /** 启动失败且资源已尽力关闭，不允许重试复用本实例。 */
    FAILED,
    /** 已完成幂等关闭。 */
    CLOSED
}
