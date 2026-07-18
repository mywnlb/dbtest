package cn.zhangyis.db.storage.api.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;

/**
 * XA resource-manager phase-two commit 命令。
 *
 * @param transaction 已持久化 PREPARED 的写事务；若上次仅 durability 等待失败，也可传入 COMMITTED 事务重试
 * @param durabilityTimeout phase-two terminal redo fsync 的有界等待时间；必须为正
 */
public record CommitPreparedTransactionCommand(Transaction transaction, Duration durabilityTimeout) {

    public CommitPreparedTransactionCommand {
        if (transaction == null) {
            throw new DatabaseValidationException("commit prepared transaction must not be null");
        }
        if (durabilityTimeout == null || durabilityTimeout.isZero() || durabilityTimeout.isNegative()) {
            throw new DatabaseValidationException("commit prepared durability timeout must be positive");
        }
    }
}
