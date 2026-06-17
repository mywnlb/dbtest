package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;

/**
 * 表空间用量快照：当前大小、freeLimit、下一个待分配 segment id。
 *
 * @param currentSizeInPages 当前物理大小页数。
 * @param freeLimitPageNo    已纳入 free-list 机制的页号上界。
 * @param nextSegmentId      下一个待分配 segment id。
 */
public record SpaceUsage(PageNo currentSizeInPages, PageNo freeLimitPageNo, long nextSegmentId) {

    public SpaceUsage {
        if (currentSizeInPages == null || freeLimitPageNo == null) {
            throw new DatabaseValidationException("space usage page fields must not be null");
        }
    }
}
