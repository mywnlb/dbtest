package cn.zhangyis.db.storage.api.dml;

/**
 * 表级 DML secondary 的包内测试故障接缝。回调只位于 unique 初检后、写 MTR 创建前或成功 commit 后，
 * 不能在页修改中间注入 Java 异常，否则 MiniTransaction 当前没有 content undo，无法表达安全重试边界。
 */
@FunctionalInterface
interface TableDmlProgressFaultInjector {

    /** 生产默认实现，不改变 DML 行为。 */
    TableDmlProgressFaultInjector NO_OP = (phase, operation, indexId) -> { };

    /**
     * 在可重试的稳定物理边界通知测试；实现可抛异常模拟进程中断，但不得改写数据库状态或保存页对象。
     *
     * @param phase     unique/row-guard 之间、写 MTR 创建前或成功提交后的稳定边界，决定故障是否已有 redo/dirty 副作用。
     * @param operation 当前 secondary insert/revive 或 delete-mark 操作类型，用于精确选择注入点。
     * @param indexId   当前目标二级索引稳定 id；测试据此区分多索引编排中的具体步骤。
     */
    void onBoundary(TableDmlProgressPhase phase, TableDmlSecondaryOperation operation, long indexId);
}
