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
    /** redo 完成后把恢复边界安装到本进程将继续使用的 RedoLogManager，使新 MTR 从 recoveredToLsn 续写、历史日志视为 durable。 */
    REDO_BOUNDARY_INSTALL,
    /** redo 后验证 system.ibd Change Buffer header/global tree；目标页保持惰性，失败不得进入 undo 或开放流量。 */
    CHANGE_BUFFER_RECOVER,
    /** redo 完成后续作 durable TRUNCATING undo 表空间，完成前保持流量关闭。 */
    UNDO_TABLESPACE_RESUME,
    /**
     * 把物理文件大小重对齐到 page0 权威逻辑大小，弥补 autoExtend 未 fsync 留下的背离。
     * 必须晚于 UNDO_TABLESPACE_RESUME：undo 续作会把被截断 undo 表空间的 page0 重建为新小尺寸，
     * 若 reconcile 抢先按旧大尺寸读 page0，会把刚截断的文件重新撑大、甚至磁盘不足阻断截断。
     */
    SPACE_FILE_RECONCILE,
    /** 回滚 crash 前未提交的 recovered ACTIVE undo 段；DD 模式按 undo identity 解析表级多索引并执行幂等 inverse。 */
    UNDO_ROLLBACK,
    /** 从 recovered COMMITTED undo header 重建 history list 和事务提交序水位，供启动后的 purge driver 续作。 */
    RESUME_PURGE,
    /** R2 必需阶段成功后开放普通用户流量。 */
    OPEN_TRAFFIC,
    /** READ_ONLY_VALIDATE 扫描完成后进入只读诊断态；不开放普通用户流量。 */
    READ_ONLY_DIAGNOSTIC_OPEN
}
