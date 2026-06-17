package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;

/**
 * Buffer Pool 暴露给 Flush 模块的脏页候选。候选只携带 flush list 排序和 WAL gate 需要的 LSN 边界，
 * 不泄漏 {@link BufferFrame}、page latch 或可变页内容，保持 flush -> buf 的协作边界。
 *
 * @param pageId 物理页定位键。
 * @param oldestModificationLsn 该页从 clean 变 dirty 时的最早修改 LSN，checkpoint 不能越过它。
 * @param newestModificationLsn 当前页头 pageLSN，数据页落盘前必须先等 redo durable 覆盖该 LSN。
 */
public record DirtyPageCandidate(PageId pageId, Lsn oldestModificationLsn, Lsn newestModificationLsn) {

    public DirtyPageCandidate {
        if (pageId == null || oldestModificationLsn == null || newestModificationLsn == null) {
            throw new DatabaseValidationException("dirty page candidate fields must not be null");
        }
        if (oldestModificationLsn.value() > newestModificationLsn.value()) {
            throw new DatabaseValidationException("dirty oldest LSN must not exceed newest LSN");
        }
    }
}
