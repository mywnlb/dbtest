package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionNo;

import java.util.Optional;

/**
 * rollback segment page3 中持久 history list base 的不可变快照。
 *
 * <p>head/tail 必须同时存在或同时为空；length 是物理链节点数，不表达 TransactionNo 排序。lastTransactionNo
 * 是曾成功挂链事务号的最大值，purge 后也不回退，用于重启时防止提交号复用。
 *
 * @param headPageId 持久链首 UPDATE undo first page；空链为空。
 * @param tailPageId 持久链尾 UPDATE undo first page；空链为空。
 * @param length 当前物理节点数。
 * @param lastTransactionNo 持久提交号高水位，0 表从未挂入 history。
 */
public record RollbackSegmentHistoryBase(Optional<PageId> headPageId,
                                         Optional<PageId> tailPageId,
                                         long length,
                                         TransactionNo lastTransactionNo) {

    public RollbackSegmentHistoryBase {
        if (headPageId == null || tailPageId == null || lastTransactionNo == null) {
            throw new DatabaseValidationException("rseg history base fields must not be null");
        }
        if (length < 0 || headPageId.isPresent() != tailPageId.isPresent()
                || (length == 0) != headPageId.isEmpty()) {
            throw new DatabaseValidationException("rseg history base endpoints/length are inconsistent");
        }
        if (headPageId.isPresent()
                && !headPageId.orElseThrow().spaceId().equals(tailPageId.orElseThrow().spaceId())) {
            throw new DatabaseValidationException("rseg history endpoints must belong to the same space");
        }
    }

    /** 新建或 truncate rebuild 后的空 history base。 */
    public static RollbackSegmentHistoryBase empty() {
        return new RollbackSegmentHistoryBase(Optional.empty(), Optional.empty(), 0L, TransactionNo.NONE);
    }
}
