package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务启动选项。
 *
 * @param isolationLevel 隔离级别（本片仅记录，不驱动行为）。
 * @param readOnly       只读事务：不分配写 id、不进活跃表、commit 不分配 transactionNo。
 * @param autoCommit     自动提交标志（本片仅记录，session 层后续消费）。
 */
public record TransactionOptions(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {

    public TransactionOptions {
        if (isolationLevel == null) {
            throw new DatabaseValidationException("isolation level must not be null");
        }
    }

    /** 默认选项：REPEATABLE_READ、读写、autocommit。 */
    public static TransactionOptions defaults() {
        return new TransactionOptions(IsolationLevel.REPEATABLE_READ, false, true);
    }
}
