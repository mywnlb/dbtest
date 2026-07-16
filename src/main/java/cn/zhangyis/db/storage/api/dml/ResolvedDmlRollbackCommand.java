package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.trx.Transaction;

/** 使用 DD undo target resolver 的完整事务回滚命令，不携带“最后一个聚簇索引”回退值。 */
public record ResolvedDmlRollbackCommand(Transaction transaction) {
    public ResolvedDmlRollbackCommand {
        if (transaction == null) {
            throw new DatabaseValidationException("resolved DML rollback transaction must not be null");
        }
    }
}
