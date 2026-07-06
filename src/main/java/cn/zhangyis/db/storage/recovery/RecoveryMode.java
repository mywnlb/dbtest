package cn.zhangyis.db.storage.recovery;

/**
 * crash recovery 模式。NORMAL 是普通可写启动恢复；READ_ONLY_VALIDATE 只扫描输入并生成诊断报告；
 * force-skip 仍是后续 force recovery 策略的稳定枚举。
 */
public enum RecoveryMode {
    /** 默认启动恢复：redo 损坏或必要物理恢复失败时 fail closed。 */
    NORMAL,
    /** 只读校验模式：扫描 doublewrite/redo 并报告诊断，不写 data file，不安装 redo 边界，也不开放普通流量。 */
    READ_ONLY_VALIDATE,
    /** force recovery 扩展点：后续可跳过配置允许的损坏 tablespace。 */
    FORCE_SKIP_CORRUPT_TABLESPACE
}
