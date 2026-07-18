package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

import java.util.Optional;

/**
 * 一张 undo first page 的 history/recovery 不可变快照。读取过程只持首页 S latch，不跟随 segment tail，返回后可安全
 * 跨 MTR 使用；prev/next 表达跨事务 history 链，而 {@link UndoSegmentHandle} 的 first/last 表达段内 FIL 页链。
 */
public record UndoHistoryNodeSnapshot(PageId firstPageId, UndoSegmentHandle handle,
                                      UndoLogKind kind, UndoLogState state,
                                      TransactionId creatorTransactionId,
                                      TransactionNo committedTransactionNo,
                                      Optional<PageId> previousHistoryPageId,
                                      Optional<PageId> nextHistoryPageId,
                                      UndoLogicalHead logicalHead) {

    public UndoHistoryNodeSnapshot {
        if (firstPageId == null || handle == null || kind == null || state == null || creatorTransactionId == null
                || committedTransactionNo == null || previousHistoryPageId == null
                || nextHistoryPageId == null || logicalHead == null) {
            throw new DatabaseValidationException("undo history node snapshot fields must not be null");
        }
        if (!firstPageId.equals(handle.firstPageId())) {
            throw new DatabaseValidationException("undo history snapshot first page/handle mismatch");
        }
        previousHistoryPageId.ifPresent(page -> requireSameSpace(firstPageId, page));
        nextHistoryPageId.ifPresent(page -> requireSameSpace(firstPageId, page));
    }

    public boolean isActive() {
        return state == UndoLogState.ACTIVE;
    }

    /** 当前 first page 是否是未决 XA participant，且仍不得进入 history。 */
    public boolean isPrepared() {
        return state == UndoLogState.PREPARED;
    }

    public boolean isCommitted() {
        return state == UndoLogState.COMMITTED;
    }

    public boolean isCached() {
        return state == UndoLogState.CACHED;
    }

    private static void requireSameSpace(PageId firstPageId, PageId linkedPageId) {
        if (!firstPageId.spaceId().equals(linkedPageId.spaceId())) {
            throw new DatabaseValidationException("undo history links must stay in one rollback segment space");
        }
    }
}
