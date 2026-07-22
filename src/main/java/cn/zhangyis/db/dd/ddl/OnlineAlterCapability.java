package cn.zhangyis.db.dd.ddl;

/** strategy selector用于解释“为何不能online”的稳定能力标识。 */
public enum OnlineAlterCapability {
    /** 同一sidecar原子持有多个ADD candidate与DROP retirement descriptor。 */
    VERSIONED_MULTI_INDEX_SIDECAR,
    /** 对任意clustered mutation记录稳定row identity的shadow change-log。 */
    SHADOW_CHANGE_LOG,
    /** cutover前等待旧ReadView/history不再依赖source row layout的屏障。 */
    MVCC_SCHEMA_RETENTION_BARRIER
}
