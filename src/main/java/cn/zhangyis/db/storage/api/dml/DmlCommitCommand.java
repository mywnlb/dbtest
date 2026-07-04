package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.DurabilityPolicy;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;

/**
 * Storage DML COMMIT 输入。durability 策略由调用方逐次显式传入，避免在尚无 session/autocommit 层时
 * 把默认提交策略藏入全局配置。
 *
 * @param transaction       待提交事务；不能为 null。
 * @param durabilityPolicy  提交返回前对 redo write/flush 的等待策略；不能为 null。
 * @param durabilityTimeout redo durability 等待上限，必须为正。
 */
public record DmlCommitCommand(Transaction transaction, DurabilityPolicy durabilityPolicy,
                               Duration durabilityTimeout) {

    public DmlCommitCommand {
        if (transaction == null) {
            throw new DatabaseValidationException("DML commit transaction must not be null");
        }
        if (durabilityPolicy == null) {
            throw new DatabaseValidationException("DML commit durabilityPolicy must not be null");
        }
        if (durabilityTimeout == null || durabilityTimeout.isZero() || durabilityTimeout.isNegative()) {
            throw new DatabaseValidationException("DML commit durabilityTimeout must be positive");
        }
    }
}
