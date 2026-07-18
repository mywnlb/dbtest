package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.trx.Transaction;

/** 使用 DD undo target resolver 的完整事务回滚命令，不携带“最后一个聚簇索引”回退值。
 *
 * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
 */
public record ResolvedDmlRollbackCommand(Transaction transaction) {
    public ResolvedDmlRollbackCommand {
        if (transaction == null) {
            throw new DatabaseValidationException("resolved DML rollback transaction must not be null");
        }
    }
}
