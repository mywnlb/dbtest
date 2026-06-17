package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.SpaceId;

import java.util.List;

/**
 * tablespace drain 结果。上层 drop/truncate/discard 集成时可据此判断目标 space 是否已经没有 dirty page。
 *
 * @param spaceId 被 drain 的表空间。
 * @param results drain 期间执行过的单页 flush 结果。
 * @param timedOut true 表示 timeout 到达时目标 space 仍可能存在 dirty page。
 * @param checkpointLsn drain 结束时 checkpoint 边界。
 */
public record TablespaceDrainResult(SpaceId spaceId,
                                    List<FlushResult> results,
                                    boolean timedOut,
                                    Lsn checkpointLsn) {

    public TablespaceDrainResult {
        if (spaceId == null || results == null || checkpointLsn == null) {
            throw new DatabaseValidationException("tablespace drain result fields must not be null");
        }
        results = List.copyOf(results);
    }
}
