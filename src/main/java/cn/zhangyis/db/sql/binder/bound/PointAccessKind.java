package cn.zhangyis.db.sql.binder.bound;

/**
 * Binder 已确定的 point-select 访问路径种类。该枚举只表达 SQL/DD 层意图；物理 B+Tree descriptor 与 compact
 * secondary layout 由 gateway 在 exact-version mapper 中解析，不能进入 SQL 包。
 */
public enum PointAccessKind {

    /** 谓词完整覆盖无 prefix 聚簇主键，直接执行聚簇 MVCC 点读。 */
    CLUSTERED_PRIMARY,

    /** 谓词完整覆盖无 prefix logical unique secondary，执行候选 scan 后回表 MVCC。 */
    UNIQUE_SECONDARY
}
