package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;

/**
 * INSERT/UPDATE/DELETE 的写入结果。它只表达本条 DML 的 MTR 边界和影响行数，
 * 不代表数据库事务已提交；事务级提交仍必须显式调用 {@link ClusteredDmlService#commit(DmlCommitCommand)}。
 *
 * @param changed       是否发生聚簇页内容变化。
 * @param affectedRows  逻辑影响行数；miss 为 0，成功写为 1。
 * @param endLsn        本次写 MTR 的 end LSN；miss/no-op 可为当前 redo LSN。
 * @param transactionId 写事务 id；写路径必须是非 NONE，结果构造只校验非 null。
 */
public record DmlWriteResult(boolean changed, int affectedRows, Lsn endLsn, TransactionId transactionId) {

    public DmlWriteResult {
        if (affectedRows < 0) {
            throw new DatabaseValidationException("DML affectedRows must not be negative: " + affectedRows);
        }
        if (endLsn == null) {
            throw new DatabaseValidationException("DML write endLsn must not be null");
        }
        if (transactionId == null) {
            throw new DatabaseValidationException("DML write transactionId must not be null");
        }
    }
}
