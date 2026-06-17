package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * update 结果值对象。
 *
 * @param outcome 结果分类。
 * @param newRef  更新后记录的定位（IN_PLACE 为原位、MOVED 为新位置）；REQUIRES_REINSERT 时为 null。
 */
public record UpdateResult(UpdateOutcome outcome, RecordRef newRef) {

    public UpdateResult {
        if (outcome == null) {
            throw new DatabaseValidationException("update outcome must not be null");
        }
        if (outcome == UpdateOutcome.REQUIRES_REINSERT && newRef != null) {
            throw new DatabaseValidationException("REQUIRES_REINSERT must carry null ref");
        }
        if (outcome != UpdateOutcome.REQUIRES_REINSERT && newRef == null) {
            throw new DatabaseValidationException(outcome + " must carry a non-null ref");
        }
    }
}
