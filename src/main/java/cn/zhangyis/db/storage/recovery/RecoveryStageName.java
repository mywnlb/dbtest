package cn.zhangyis.db.storage.recovery;

/**
 * R2 crash recovery 阶段名。阶段顺序写入 {@link RecoveryReport}，用于测试和启动诊断。
 */
public enum RecoveryStageName {
    /** 关闭普通用户流量入口。 */
    TRAFFIC_CLOSED,
    /** 先用 doublewrite 修复 torn page。 */
    DOUBLEWRITE_REPAIR,
    /** 从 checkpoint 后扫描并重放 redo。 */
    REDO_REPLAY,
    /** R2 必需阶段成功后开放普通用户流量。 */
    OPEN_TRAFFIC
}
