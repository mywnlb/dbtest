package cn.zhangyis.db.storage.redo;

/**
 * redo 预算的领域用途。用途只用于准入诊断和低估定位，不进入 redo 持久格式，也不参与恢复分派。
 * 每个生产写 MTR 必须选择能表达其最坏分支的用途，禁止用匿名固定额度掩盖估算责任。
 */
public enum RedoBudgetPurpose {
    /** 只读 MTR；其逻辑和物理预算必须同时为零。 */
    READ_ONLY,
    /** 引擎首次创建系统页或修复固定布局元数据。 */
    ENGINE_BOOT,
    /** 聚簇索引插入及同批 undo 写。 */
    CLUSTERED_INSERT,
    /** 聚簇索引更新及同批 undo 写。 */
    CLUSTERED_UPDATE,
    /** 聚簇索引 delete-mark 及同批 undo 写。 */
    CLUSTERED_DELETE,
    /** 二级索引单树 publish/revive/delete-mark/purge 物理 MTR。 */
    SECONDARY_INDEX,
    /** purge 对聚簇索引执行物理删除。 */
    PURGE_INDEX,
    /** purge 原子消费一条 undo record 的旧 LOB ownership 并推进持久 logical head。 */
    PURGE_RECORD_PROGRESS,
    /** rollback 应用一条 inverse record。 */
    ROLLBACK_INVERSE,
    /** rollback 推进 undo logical head。 */
    ROLLBACK_MARKER,
    /** 事务 ACTIVE/COMMITTED/ROLLED_BACK 等恢复状态转换。 */
    TRANSACTION_STATE,
    /** undo segment 提交状态写入。 */
    UNDO_COMMIT,
    /** undo segment drop、rseg slot clear 与事务终态的原子批次。 */
    UNDO_FINALIZATION,
    /** undo tablespace 生命周期 marker。 */
    UNDO_TRUNCATE_LIFECYCLE,
    /** undo tablespace FSP 固定布局重建。 */
    UNDO_TRUNCATE_REBUILD,
    /** 写入并格式化一条 off-page LOB 页链。 */
    LOB_WRITE,
    /** 校验并释放一条 off-page LOB 页链。 */
    LOB_FREE,
    /** CREATE TABLE 初始化 GENERAL/FSP、index segment 与 root；上界随索引数动态计算。 */
    DDL_TABLE_CREATE,
    /** DROP TABLE 写 page0 DISCARDED lifecycle marker。 */
    DDL_TABLE_DROP,
    /** 写或修复 GENERAL 表空间 page0/page3 SDI 快照。 */
    DDL_SDI_WRITE,
    /** 回收未发布 CREATE INDEX 的两个 segment，并清空 page3 build descriptor。 */
    DDL_INDEX_DROP,
    /** 仅供 no-op manager 的页原语测试；生产 capacity-aware manager 禁止隐式使用。 */
    TEST_UNBOUNDED
}
