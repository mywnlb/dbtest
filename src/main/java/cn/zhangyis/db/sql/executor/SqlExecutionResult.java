package cn.zhangyis.db.sql.executor;

/** SQL 执行公开结果的封闭层次；不会暴露 Transaction、LogicalRecord 或任何物理引用。 */
public sealed interface SqlExecutionResult permits QueryResult, UpdateResult, CommandResult {
    /** 返回语句结束后的 Session 事务快照。 */
    TransactionStatus transactionStatus();
}
