package cn.zhangyis.db.engine;

/** 数据库组合根对外发布的访问能力；它独立于 crash recovery 阶段状态，启动完成后保持稳定。 */
public enum DatabaseAccessMode {
    /** 无隔离对象的正常读写实例。 */
    NORMAL,
    /** 已持久隔离部分对象，其余健康对象继续正常读写。 */
    DEGRADED,
    /** 本次 FORCE 启动只允许导出健康对象和事务控制，不允许任何写入。 */
    RECOVERY_EXPORT_READ_ONLY,
    /** 只扫描恢复证据的验证实例，不应用 redo/undo 或开放普通数据访问。 */
    VALIDATION_READ_ONLY
}
