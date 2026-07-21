package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * XA phase one 前从 opaque handle 读取的最小资源身份。
 *
 * @param transactionId 有写分支的正 storage transaction id；只读分支固定为 0
 * @param hasWrites 是否存在需要 PREPARED 的 durable undo/write owner
 */
public record SqlXaTransactionIdentity(long transactionId, boolean hasWrites) {

    public SqlXaTransactionIdentity {
        if (transactionId < 0 || hasWrites != (transactionId > 0)) {
            throw new DatabaseValidationException("XA transaction identity/write flag mismatch");
        }
    }
}
