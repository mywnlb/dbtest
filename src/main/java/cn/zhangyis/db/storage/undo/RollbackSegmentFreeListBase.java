package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.Optional;

/**
 * rollback segment page3 中持久 free undo segment FIFO 的不可变 base。
 *
 * <p>head/tail 必须同时存在或同时为空；length 是权威物理节点数，恢复严格按该值遍历，避免损坏链导致无界读取。
 *
 * @param headPageId 队首 FREE undo first page；空队列为空。
 * @param tailPageId 队尾 FREE undo first page；空队列为空。
 * @param length     当前持久节点数。
 */
public record RollbackSegmentFreeListBase(Optional<PageId> headPageId,
                                          Optional<PageId> tailPageId,
                                          long length) {

    public RollbackSegmentFreeListBase {
        if (headPageId == null || tailPageId == null) {
            throw new DatabaseValidationException("rseg free-list endpoints must not be null");
        }
        if (length < 0 || headPageId.isPresent() != tailPageId.isPresent()
                || (length == 0) != headPageId.isEmpty()) {
            throw new DatabaseValidationException("rseg free-list endpoints/length are inconsistent");
        }
        if (length > Integer.MAX_VALUE) {
            throw new DatabaseValidationException("rseg free-list length exceeds runtime directory limit: " + length);
        }
        if (headPageId.isPresent()
                && !headPageId.orElseThrow().spaceId().equals(tailPageId.orElseThrow().spaceId())) {
            throw new DatabaseValidationException("rseg free-list endpoints must belong to the same space");
        }
    }

    /** 新建或 truncate rebuild 后的空 free list。 */
    public static RollbackSegmentFreeListBase empty() {
        return new RollbackSegmentFreeListBase(Optional.empty(), Optional.empty(), 0L);
    }
}
