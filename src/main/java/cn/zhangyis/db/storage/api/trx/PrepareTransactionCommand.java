package cn.zhangyis.db.storage.api.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;

/**
 * XA resource-manager phase-one 命令。外部协调器负责持久化 XID 与事务 id 的映射；存储层只接收当前实例内
 * 已产生普通 undo 的写事务，并保证返回前 PREPARED redo 已经 fsync。
 *
 * @param transaction 待进入 PREPARED 的 ACTIVE 写事务；必须已分配 write id 且拥有普通 undo
 * @param durabilityTimeout phase-one redo fsync 的有界等待时间；必须为正
 */
public record PrepareTransactionCommand(Transaction transaction, Duration durabilityTimeout) {

    public PrepareTransactionCommand {
        if (transaction == null) {
            throw new DatabaseValidationException("prepare transaction must not be null");
        }
        if (durabilityTimeout == null || durabilityTimeout.isZero() || durabilityTimeout.isNegative()) {
            throw new DatabaseValidationException("prepare durability timeout must be positive");
        }
    }
}
