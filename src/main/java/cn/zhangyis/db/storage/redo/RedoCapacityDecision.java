package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * redo capacity policy 的一次判断结果。
 *
 * @param pressure 当前 checkpoint age 对应的压力级别。
 * @param checkpointAgeBytes {@code currentLsn - checkpointLsn}，即恢复可能需要扫描的 redo 字节跨度。
 * @param targetCheckpointLsn 建议 flush/checkpoint 推进到的目标；R2 只报告，不主动调度刷脏。
 */
public record RedoCapacityDecision(RedoCapacityPressure pressure, long checkpointAgeBytes, Lsn targetCheckpointLsn) {

    public RedoCapacityDecision {
        if (pressure == null || targetCheckpointLsn == null) {
            throw new DatabaseValidationException("redo capacity decision fields must not be null");
        }
        if (checkpointAgeBytes < 0) {
            throw new DatabaseValidationException("redo checkpoint age must not be negative: " + checkpointAgeBytes);
        }
    }
}
