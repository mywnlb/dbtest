package cn.zhangyis.db.dd.ddl;

/** 通用ALTER策略选择的稳定诊断原因；测试与可观察层不得依赖自由文本。 */
public enum OnlineAlterReason {
    /** 全部action均只改变DD canonical image。 */
    METADATA_ONLY_ACTIONS,
    /** 单ADD或单DROP可复用已接线的Online Index协议。 */
    SINGLE_INDEX_ONLINE_PROTOCOL,
    /** 多索引原子owner尚未接入生产sidecar。 */
    MULTI_INDEX_SIDECAR_PENDING,
    /** metadata与index混合需要通用manifest才能保持单aggregate发布。 */
    MIXED_ACTION_MANIFEST_PENDING,
    /** row-layout变更的change-log与旧schema MVCC边界尚未同时接线。 */
    SHADOW_REBUILD_PROTOCOL_PENDING,
    /** 多索引或metadata/index混合动作由一个manifest与table-level gate原子拥有。 */
    GENERAL_INPLACE_MANIFEST,
    /** record layout变化由shadow copy、clustered identity日志与MVCC barrier完成。 */
    SHADOW_REBUILD_PROTOCOL
}
