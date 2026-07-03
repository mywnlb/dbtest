package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Buffer Pool 观察到的表空间生命周期版本。
 *
 * <p>该值只表达内存中 frame admission 的代际，不是磁盘 page0 lifecycle marker，也不进入 redo。truncate/drop/discard
 * 完成 Buffer Pool drain 后推进版本，使旧 frame 即使仍残留在 hash/LRU 的短窗口中，也不能再被普通读路径返回。
 *
 * @param value 单调递增的非负代际；0 表示进程内首次访问该表空间的初始版本。
 */
record TablespaceVersion(long value) {

    /** 进程内初始表空间版本。 */
    static final TablespaceVersion INITIAL = new TablespaceVersion(0);

    TablespaceVersion {
        if (value < 0) {
            throw new DatabaseValidationException("tablespace version must be >= 0: " + value);
        }
    }

    /**
     * 返回下一代版本。只由 {@link SpaceLifecycleClock} 在 invalidate 全部分片 drain+clean 后调用。
     *
     * @return 当前版本的下一代。
     */
    TablespaceVersion next() {
        if (value == Long.MAX_VALUE) {
            throw new DatabaseValidationException("tablespace version overflow");
        }
        return new TablespaceVersion(value + 1);
    }
}
