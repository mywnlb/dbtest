package cn.zhangyis.db.storage.api.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;

/**
 * XA resource-manager phase-two rollback 命令。当前兼容模式显式携带聚簇索引；使用 DD exact-version resolver
 * 的生产组合根可在后续增加不暴露物理索引的命令变体。
 *
 * @param transaction PREPARED/PREPARED_ROLLING_BACK 写事务；终态 durability 重试时也可为 ROLLED_BACK
 * @param clusteredIndex undo 解码与反向应用所需的稳定聚簇索引快照
 * @param durabilityTimeout phase-two terminal redo fsync 的有界等待时间；必须为正
 */
public record RollbackPreparedTransactionCommand(Transaction transaction,
                                                 BTreeIndex clusteredIndex,
                                                 Duration durabilityTimeout) {

    public RollbackPreparedTransactionCommand {
        if (transaction == null || clusteredIndex == null) {
            throw new DatabaseValidationException(
                    "rollback prepared transaction/index must not be null");
        }
        if (durabilityTimeout == null || durabilityTimeout.isZero() || durabilityTimeout.isNegative()) {
            throw new DatabaseValidationException("rollback prepared durability timeout must be positive");
        }
    }
}
