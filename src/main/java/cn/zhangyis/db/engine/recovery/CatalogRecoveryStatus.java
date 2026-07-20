package cn.zhangyis.db.engine.recovery;

/**
 * 一次离线 inspection 的顶层裁决。
 */
public enum CatalogRecoveryStatus {
    /** catalog 仍可严格打开，不应执行 quarantine/rebuild。 */
    NO_RECOVERY_NEEDED,
    /** 存在未隔离证据、缺失/损坏 expected 文件或不完整 manifest。 */
    BLOCKED,
    /** valid clean manifest 与完整候选集一致，可显式执行 rebuild。 */
    REBUILDABLE
}
