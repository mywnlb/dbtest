package cn.zhangyis.db.sql.executor;

/** SQL 执行公开结果的封闭层次；不会暴露 Transaction、LogicalRecord 或任何物理引用。 */
public sealed interface SqlExecutionResult permits QueryResult, UpdateResult, CommandResult {
    /** 返回语句结束后的 Session 事务快照。
     *
     * @return {@code transactionStatus} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    TransactionStatus transactionStatus();
}
