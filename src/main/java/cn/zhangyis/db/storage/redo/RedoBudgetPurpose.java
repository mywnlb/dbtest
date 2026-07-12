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
    /** purge 对聚簇索引执行物理删除。 */
    PURGE_INDEX,
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
    /** 仅供 no-op manager 的页原语测试；生产 capacity-aware manager 禁止隐式使用。 */
    TEST_UNBOUNDED
}
