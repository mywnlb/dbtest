package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;

/**
 * History list 条目（设计 §5.6）：一条已提交、含 update/delete undo 的事务 undo log。purge 据 {@code transactionNo}
 * 判 boundary（{@code < purgeLowWaterNo} 可回收）、据 {@code creatorTrxId} 校验 delete-marked 记录所有权、经
 * {@code undoFirstPageId} 打开 undo 段遍历 DELETE_MARK 记录物理移除，全部成功后以 {@code slotId + firstPageId}
 * 作为 owner identity 原子 cache/drop 段并转移 page3 owner，最后发布内存 slot/cache 状态。
 *
 * @param transactionNo   提交序号（FIFO/boundary 依据）。
 * @param creatorTrxId    写该 undo log 的事务 id（= delete-marked 记录应有的 DB_TRX_ID）。
 * @param undoSpaceId     undo 表空间。
 * @param undoFirstPageId undo 段链首页（purge 打开段、回收时取 handle 用）。
 * @param slotId          page3 与内存目录中的 rseg slot identity（purge 终结时做 owner CAS）。
 */
public record HistoryEntry(TransactionNo transactionNo, TransactionId creatorTrxId, SpaceId undoSpaceId,
                           PageId undoFirstPageId, UndoSlotId slotId) {

    public HistoryEntry {
        if (transactionNo == null || creatorTrxId == null || undoSpaceId == null
                || undoFirstPageId == null || slotId == null) {
            throw new DatabaseValidationException("history entry fields must not be null");
        }
        if (transactionNo.isNone() || creatorTrxId.isNone()) {
            throw new DatabaseValidationException("history entry transaction no/id must not be NONE");
        }
        if (!undoSpaceId.equals(undoFirstPageId.spaceId())) {
            throw new DatabaseValidationException("history entry first page must belong to undo space: "
                    + undoFirstPageId);
        }
    }
}
