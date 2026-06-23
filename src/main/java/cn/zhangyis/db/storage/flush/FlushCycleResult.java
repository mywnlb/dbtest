package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.flush.policy.FlushAdvice;
import cn.zhangyis.db.storage.redo.RedoCapacityDecision;

import java.util.List;

/**
 * 一轮 flush service 调度结果。它记录 capacity 判断、策略建议、实际单页结果以及 checkpoint 前后边界。
 *
 * @param capacityDecision redo capacity pressure 判断。
 * @param advice adaptive flush 建议。
 * @param results 实际 flush 结果；pressure 为 NONE 时可以为空。
 * @param checkpointBefore 本轮前 checkpoint LSN。
 * @param checkpointAfter 本轮后 checkpoint LSN。
 */
public record FlushCycleResult(RedoCapacityDecision capacityDecision,
                               FlushAdvice advice,
                               List<FlushResult> results,
                               Lsn checkpointBefore,
                               Lsn checkpointAfter) {

    public FlushCycleResult {
        if (capacityDecision == null || advice == null || results == null
                || checkpointBefore == null || checkpointAfter == null) {
            throw new DatabaseValidationException("flush cycle result fields must not be null");
        }
        if (checkpointAfter.value() < checkpointBefore.value()) {
            throw new DatabaseValidationException("checkpoint must not move backward in a flush cycle");
        }
        results = List.copyOf(results);
    }
}
