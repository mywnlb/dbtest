package cn.zhangyis.db.storage.api.dml;

/** 表级 DML secondary 短写 MTR 的测试故障边界；生产默认不注入动作。 */
enum TableDmlProgressPhase {
    /** root snapshot 已刷新，但目标 secondary 写 MTR 尚未创建。 */
    BEFORE_MTR,
    /** 目标 secondary 写 MTR 已成功提交，page/redo/dirty 状态已发布。 */
    AFTER_COMMIT
}
