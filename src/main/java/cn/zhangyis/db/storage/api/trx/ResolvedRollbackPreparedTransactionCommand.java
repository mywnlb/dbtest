package cn.zhangyis.db.storage.api.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;

/**
 * DD exact-version resolver 模式的 prepared rollback 命令。命令不暴露 B+Tree/page 等内部结构，undo record
 * 自身携带的 table/index identity 由组合根解析为权威回滚目标。
 *
 * @param transaction PREPARED/PREPARED_ROLLING_BACK 写事务；durability 确认重试时可为 ROLLED_BACK
 * @param durabilityTimeout phase-two terminal redo fsync 的正有界等待时间
 */
public record ResolvedRollbackPreparedTransactionCommand(
        Transaction transaction, Duration durabilityTimeout) {

    public ResolvedRollbackPreparedTransactionCommand {
        if (transaction == null) {
            throw new DatabaseValidationException(
                    "resolved rollback prepared transaction must not be null");
        }
        if (durabilityTimeout == null || durabilityTimeout.isZero() || durabilityTimeout.isNegative()) {
            throw new DatabaseValidationException(
                    "resolved rollback prepared durability timeout must be positive");
        }
    }
}
