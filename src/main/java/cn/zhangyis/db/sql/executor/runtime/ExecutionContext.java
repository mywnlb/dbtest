package cn.zhangyis.db.sql.executor.runtime;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.sql.executor.storage.SqlCursorScope;

import java.util.Optional;

/**
 * 单条语句私有的 Executor 运行上下文；不缓存到 PhysicalPlan，也不跨 statement 复用。
 *
 * @param transaction 当前 Session 持有的不透明事务能力
 * @param deadline 从 parse 开始共享的唯一绝对语句期限
 */
public record ExecutionContext(
        SqlTransactionHandle transaction,
        SqlStatementDeadline deadline,
        Optional<SqlCursorScope> cursorScope) {

    /**
     * 拒绝缺失运行能力，保证节点打开失败早于 storage cursor 创建。
     *
     * @throws DatabaseValidationException transaction/deadline 缺失时抛出
     */
    public ExecutionContext {
        if (transaction == null || deadline == null
                || cursorScope == null) {
            throw new DatabaseValidationException(
                    "execution context transaction/deadline must not be null");
        }
    }

    /**
     * 保留节点单测与旧直接构造入口；没有 scope 时访问叶回退到 Data Port 旧 cursor 方法。
     */
    public ExecutionContext(
            SqlTransactionHandle transaction,
            SqlStatementDeadline deadline) {
        this(transaction, deadline, Optional.empty());
    }
}
