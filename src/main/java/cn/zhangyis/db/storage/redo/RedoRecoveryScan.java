package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.util.List;

/**
 * Repository 一次原子恢复扫描的逻辑边界。{@code retainedStartLsn} 来自单文件固定起点或 ring 最早 in-use
 * 文件 header；即使该文件只剩 torn chain、没有完整 batch，也不能丢掉这个非零 checkpoint 边界。
 *
 * @param batches 完整且可交给 replay 的批次。
 * @param retainedStartLsn 当前物理输入声明的最早逻辑 LSN。
 * @param endLsn 最后完整批次后的 LSN；空批时必须等于 retainedStartLsn。
 */
public record RedoRecoveryScan(List<RedoLogBatch> batches, Lsn retainedStartLsn, Lsn endLsn) {

    public RedoRecoveryScan {
        if (batches == null || retainedStartLsn == null || endLsn == null) {
            throw new DatabaseValidationException("redo recovery scan fields must not be null");
        }
        if (endLsn.value() < retainedStartLsn.value()) {
            throw new DatabaseValidationException("redo recovery scan end precedes retained start");
        }
        batches = List.copyOf(batches);
        if (batches.isEmpty()) {
            if (!endLsn.equals(retainedStartLsn)) {
                throw new DatabaseValidationException(
                        "empty redo recovery scan must end at retained start");
            }
        } else if (!batches.getFirst().range().start().equals(retainedStartLsn)
                || !batches.getLast().range().end().equals(endLsn)) {
            throw new DatabaseValidationException(
                    "redo recovery scan boundaries do not match first/last batch");
        }
    }
}
