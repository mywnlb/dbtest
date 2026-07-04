package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.trx.Transaction;

/**
 * Storage DML ROLLBACK 输入。当前 rollback 依赖调用方传入本事务写入的单聚簇索引快照；
 * 多表、多索引和 DD 恢复后续再由字典层提供定位。
 *
 * @param transaction    待回滚事务；不能为 null。
 * @param clusteredIndex 单聚簇索引快照；必须是 clustered index。
 */
public record DmlRollbackCommand(Transaction transaction, BTreeIndex clusteredIndex) {

    public DmlRollbackCommand {
        if (transaction == null) {
            throw new DatabaseValidationException("DML rollback transaction must not be null");
        }
        if (clusteredIndex == null) {
            throw new DatabaseValidationException("DML rollback clusteredIndex must not be null");
        }
        if (!clusteredIndex.clustered()) {
            throw new DatabaseValidationException(
                    "DML rollback requires clustered index: " + clusteredIndex.indexId());
        }
    }
}
