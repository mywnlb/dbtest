package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogKind;

/**
 * page3 slot 与 undo first-page header 的无 latch 恢复快照。
 *
 * <p>StorageEngine 在短只读 MTR 中构造本对象并立即释放 guard，reconciler/rollback/history 阶段只消费值，
 * 不会在 undo latch 下进入 B+Tree 或事务锁等待。
 *
 * @param slotId page3 slot。
 * @param firstPageId undo segment first page。
 * @param state first-page ACTIVE/PREPARED/COMMITTED 状态。
 * @param creatorTransactionId first-page creator 写事务 id。
 * @param transactionNo COMMITTED 的提交号；ACTIVE 为 NONE。
 */
public record RecoveredUndoSlotEvidence(UndoSlotId slotId,
                                       PageId firstPageId,
                                       UndoLogKind kind,
                                       RecoveredUndoState state,
                                       TransactionId creatorTransactionId,
                                       TransactionNo transactionNo) {

    public RecoveredUndoSlotEvidence {
        if (slotId == null || firstPageId == null || kind == null || state == null
                || creatorTransactionId == null || transactionNo == null) {
            throw new DatabaseValidationException("recovered undo slot evidence fields must not be null");
        }
        if (creatorTransactionId.isNone()) {
            throw new TransactionRecoveryException(
                    "recovered undo slot has NONE creator transaction id: slot=" + slotId.value());
        }
        if (kind == UndoLogKind.TEMPORARY) {
            throw new TransactionRecoveryException("ordinary page3 slot cannot contain TEMPORARY undo");
        }
        if ((state == RecoveredUndoState.ACTIVE || state == RecoveredUndoState.PREPARED)
                && !transactionNo.isNone()) {
            throw new TransactionRecoveryException(
                    state + " recovered undo slot has commit number: slot=" + slotId.value());
        }
        if (state == RecoveredUndoState.COMMITTED && transactionNo.isNone()) {
            throw new TransactionRecoveryException(
                    "COMMITTED recovered undo slot has no commit number: slot=" + slotId.value());
        }
        if (state == RecoveredUndoState.COMMITTED && kind != UndoLogKind.UPDATE) {
            throw new TransactionRecoveryException("COMMITTED page3 slot must be UPDATE undo: slot="
                    + slotId.value() + ", kind=" + kind);
        }
    }

    /** 构造未提交的 ACTIVE slot 证据。 */
    public static RecoveredUndoSlotEvidence active(
            UndoSlotId slotId, PageId firstPageId, UndoLogKind kind, TransactionId creatorTransactionId) {
        return new RecoveredUndoSlotEvidence(slotId, firstPageId, kind, RecoveredUndoState.ACTIVE,
                creatorTransactionId, TransactionNo.NONE);
    }

    /** 构造已完成 phase one、尚待上层决议的 PREPARED slot 证据。 */
    public static RecoveredUndoSlotEvidence prepared(
            UndoSlotId slotId, PageId firstPageId, UndoLogKind kind, TransactionId creatorTransactionId) {
        return new RecoveredUndoSlotEvidence(slotId, firstPageId, kind, RecoveredUndoState.PREPARED,
                creatorTransactionId, TransactionNo.NONE);
    }

    /** 构造已提交、待 history rebuild 的 slot 证据。 */
    public static RecoveredUndoSlotEvidence committed(
            UndoSlotId slotId, PageId firstPageId, UndoLogKind kind, TransactionId creatorTransactionId,
            TransactionNo transactionNo) {
        return new RecoveredUndoSlotEvidence(slotId, firstPageId, kind, RecoveredUndoState.COMMITTED,
                creatorTransactionId, transactionNo);
    }
}
