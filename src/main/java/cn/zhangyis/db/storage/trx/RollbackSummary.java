package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 一次 rollback 的结果摘要（设计 §11.2）。T1.3d 仅记录本次回滚应用（反向走链消费）的 undo record 条数；
 * 后续片可扩展行数、savepoint 边界、失败原因等。
 *
 * @param undoRecordsApplied 本次回滚反向走链消费的 undo record 条数（含 orphan undo 的幂等 no-op 删除）；
 *                           只读/未写事务为 0。
 */
public record RollbackSummary(int undoRecordsApplied) {

    public RollbackSummary {
        if (undoRecordsApplied < 0) {
            throw new DatabaseValidationException("undoRecordsApplied must not be negative: " + undoRecordsApplied);
        }
    }
}
