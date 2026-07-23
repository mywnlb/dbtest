package cn.zhangyis.db.storage.api.dml;

/** 表级 DML secondary unique 检查与短写 MTR 的测试故障边界；生产默认不注入动作。 */
enum TableDmlProgressPhase {
    /** logical unique 锁与初次候选扫描已完成，但目标聚簇 row guard 尚未取得；用于复现并发 purge。 */
    AFTER_UNIQUE_CHECK_BEFORE_ROW_GUARD,
    /** root snapshot 已刷新，但目标 secondary 写 MTR 尚未创建。 */
    BEFORE_MTR,
    /** 目标 secondary 写 MTR 已成功提交，page/redo/dirty 状态已发布。 */
    AFTER_COMMIT
}
