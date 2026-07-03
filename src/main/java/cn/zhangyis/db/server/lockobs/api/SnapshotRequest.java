package cn.zhangyis.db.server.lockobs.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * lockobs 快照请求。第一阶段只实现当前 row-lock 诊断行；保留 includeLockData/maxRows/strictConsistency
 * 是为了后续接 SQL 视图、脱敏和大小限制时不再改公开入口。
 *
 * @param includeLockData   是否填充 LOCK_DATA 诊断摘要。
 * @param maxRows           单次快照最多返回的 data_locks 行数。
 * @param strictConsistency 是否要求严格一致；本片仅记录，超限时返回 truncated 标记。
 */
public record SnapshotRequest(boolean includeLockData, int maxRows, boolean strictConsistency) {

    public SnapshotRequest {
        if (maxRows <= 0) {
            throw new DatabaseValidationException("snapshot maxRows must be positive: " + maxRows);
        }
    }

    /** 默认诊断查询：包含 lock data，最多 1000 行，不要求全局暂停式强一致。 */
    public static SnapshotRequest defaults() {
        return new SnapshotRequest(true, 1000, false);
    }
}
