package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;

/**
 * 纯 insert-only 已提交事务的 undo 段回收条目（设计 §7.2/§7.4）。insert undo 提交即不再被任何一致性读需要，
 * rseg slot 已在 {@code UndoLogManager.onCommit} 释放；purge 仅需经 {@code undoFirstPageId} 打开段、
 * {@code dropUndoSegment} 归还其页给 FSP（无 boundary、无记录处理）。
 *
 * @param undoSpaceId     undo 表空间。
 * @param undoFirstPageId undo 段链首页。
 */
public record InsertReclaimEntry(SpaceId undoSpaceId, PageId undoFirstPageId) {

    public InsertReclaimEntry {
        if (undoSpaceId == null || undoFirstPageId == null) {
            throw new DatabaseValidationException("insert reclaim entry fields must not be null");
        }
    }
}
