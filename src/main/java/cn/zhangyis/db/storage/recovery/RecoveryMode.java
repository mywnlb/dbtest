package cn.zhangyis.db.storage.recovery;

/**
 * crash recovery 模式。R2 facade 先实现 NORMAL 的物理恢复路径，其余模式作为后续 force recovery 策略的稳定枚举。
 */
public enum RecoveryMode {
    /** 默认启动恢复：redo 损坏或必要物理恢复失败时 fail closed。 */
    NORMAL,
    /** 只读校验模式扩展点：后续只扫描诊断，不写 data file。 */
    READ_ONLY_VALIDATE,
    /** force recovery 扩展点：后续可跳过配置允许的损坏 tablespace。 */
    FORCE_SKIP_CORRUPT_TABLESPACE
}
