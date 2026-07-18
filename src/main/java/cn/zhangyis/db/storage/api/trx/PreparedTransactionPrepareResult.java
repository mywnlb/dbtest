package cn.zhangyis.db.storage.api.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;

/**
 * phase-one 成功结果；返回该对象表示 PREPARED page/redo 已经达到持久介质。
 *
 * @param transactionId 外部协调器应与 XID 一同持久化的存储事务 id
 * @param durableLsn 已由 redo fsync 覆盖的 phase-one 结束 LSN
 */
public record PreparedTransactionPrepareResult(TransactionId transactionId, Lsn durableLsn) {

    public PreparedTransactionPrepareResult {
        if (transactionId == null || transactionId.isNone() || durableLsn == null) {
            throw new DatabaseValidationException("prepared transaction result fields are invalid");
        }
    }
}
