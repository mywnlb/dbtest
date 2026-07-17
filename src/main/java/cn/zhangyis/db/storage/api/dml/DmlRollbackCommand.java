package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.trx.Transaction;

/**
 * 低层单聚簇 Storage DML ROLLBACK 输入。该兼容命令只适用于 undo 中没有 secondary mutation 的事务；
 * 表级生产写入必须使用 {@link ResolvedDmlRollbackCommand}，由 exact-version resolver 恢复全部索引与 LOB binding。
 *
 * @param transaction    待回滚事务；不能为 null。
 * @param clusteredIndex 单聚簇索引快照；必须是 clustered index。
 */
public record DmlRollbackCommand(Transaction transaction, BTreeIndex clusteredIndex) {

    /**
     * 校验低层 rollback 所需的事务与唯一聚簇 descriptor；本构造器不读取 undo 或取得锁。
     *
     * @param transaction    待回滚的写事务，不能为 {@code null}；状态合法性由 rollback service 进一步核对。
     * @param clusteredIndex 未写 secondary tail 的目标聚簇索引快照，不能为 {@code null} 且必须声明 clustered。
     * @throws DatabaseValidationException 字段缺失或错误传入非聚簇索引时抛出，不产生恢复副作用。
     */
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
