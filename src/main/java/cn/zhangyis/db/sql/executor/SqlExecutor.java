package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStorageException;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPlan;

/**
 * 执行 SQL 物理计划的稳定边界。Session 负责事务策略和 metadata lease，Executor 只把计划映射为
 * Data Port 调用与公开结果。
 */
public interface SqlExecutor {

    /**
     * 在调用方事务及 deadline 内完整执行一条物理计划。
     *
     * @param transaction 当前 Session 拥有的有效不透明事务能力
     * @param plan Compiler 已成功生成且 metadata scope 已发布的物理计划
     * @param status 执行前 Session 事务状态
     * @param deadline 当前语句唯一绝对期限
     * @return 完整查询结果或写入结果；不会泄露存储内部引用
     * @throws DatabaseValidationException transaction、plan、status 或 deadline 缺失时抛出
     * @throws SqlStorageException Data Port 无法完成完整访问时抛出；查询不返回 partial rows
     */
    SqlExecutionResult execute(
            SqlTransactionHandle transaction, PhysicalPlan plan,
            TransactionStatus status, SqlStatementDeadline deadline);
}
