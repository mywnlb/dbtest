package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;

/**
 * undo 固定前缀中无需 schema 即可读取的身份。rollback/purge 先据 tableId/indexId 选择元数据，再做完整 typed decode。
 */
public record UndoRecordIdentity(UndoRecordType type, UndoNo undoNo, TransactionId transactionId,
                                 long tableId, long indexId) {
    public UndoRecordIdentity {
        if (type == null || undoNo == null || transactionId == null || undoNo.isNone()
                || transactionId.isNone() || tableId <= 0 || indexId <= 0) {
            throw new DatabaseValidationException("invalid undo record identity");
        }
    }
}
