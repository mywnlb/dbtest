package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 已进入 rollback segment cache 的单页 undo segment 定位。kind 决定它只能服务 INSERT 或 UPDATE 首写，
 * handle 保存 FSP inode identity，复用和 truncate drop 都必须再次与首页/FSP 权威状态交叉校验。
 *
 * @param kind INSERT 或 UPDATE cache kind。
 * @param handle 单页 segment 的稳定物理定位。
 */
public record CachedUndoSegmentRef(UndoLogKind kind, UndoSegmentHandle handle) {

    public CachedUndoSegmentRef {
        if (kind == null || handle == null) {
            throw new DatabaseValidationException("cached undo segment fields must not be null");
        }
        if (kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("temporary undo segment cannot enter ordinary cache");
        }
        if (!handle.firstPageId().equals(handle.lastPageId())) {
            throw new DatabaseValidationException("cached undo segment must have one ordinary undo page");
        }
    }
}
