package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 已确认完整回滚结果。
 *
 * @param undoneRecords 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
 * @param releasedLockCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 */
public record SqlRollbackOutcome(int undoneRecords, int releasedLockCount) {
    public SqlRollbackOutcome {
        if (undoneRecords < 0 || releasedLockCount < 0) {
            throw new DatabaseValidationException("invalid SQL rollback outcome counters");
        }
    }
}
