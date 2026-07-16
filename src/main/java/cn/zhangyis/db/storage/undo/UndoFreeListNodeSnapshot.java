package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.Optional;

/**
 * 持久 free FIFO 节点的只读快照；kind 仅保留诊断含义，不限制下一次激活类型。
 *
 * @param segment            单页 segment 定位。
 * @param retainedKind       最近一次 ACTIVE 时的 kind。
 * @param previousFreePageId 前驱节点；队首为空。
 * @param nextFreePageId     后继节点；队尾为空。
 */
public record UndoFreeListNodeSnapshot(FreeUndoSegmentRef segment,
                                       UndoLogKind retainedKind,
                                       Optional<PageId> previousFreePageId,
                                       Optional<PageId> nextFreePageId) {

    public UndoFreeListNodeSnapshot {
        if (segment == null || retainedKind == null || previousFreePageId == null || nextFreePageId == null) {
            throw new DatabaseValidationException("undo free-list node fields must not be null");
        }
        if (retainedKind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("temporary undo cannot enter persistent free list");
        }
    }

    /** 返回节点 owner 的稳定首页，用于 recovery 的 cycle/duplicate 检查与 page3 端点核对。 */
    public PageId firstPageId() {
        return segment.handle().firstPageId();
    }
}
