package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 已进入 rollback segment free FIFO 的单页 undo segment 定位。free owner 与 kind 无关，激活时可重新分类。
 *
 * @param handle 保留 FSP inode identity 的稳定物理定位。
 */
public record FreeUndoSegmentRef(UndoSegmentHandle handle) {

    public FreeUndoSegmentRef {
        if (handle == null) {
            throw new DatabaseValidationException("free undo segment handle must not be null");
        }
        if (!handle.firstPageId().equals(handle.lastPageId())) {
            throw new DatabaseValidationException("free undo segment must have one ordinary undo page");
        }
    }
}
