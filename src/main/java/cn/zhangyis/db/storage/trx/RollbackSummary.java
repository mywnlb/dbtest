package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 一次 rollback 的结果摘要（设计 §11.2）。T1.3d 仅记录本次回滚应用（反向走链消费）的 undo record 条数；
 * 后续片可扩展行数、savepoint 边界、失败原因等。
 *
 * @param undoRecordsApplied 本次回滚实际反向应用到健康数据对象的 undo record 条数；
 *                           只读/未写事务为 0。
 * @param recoveryUnavailableRecordsSkipped 启动恢复已完整解码校验、但因目标表隔离而只推进 logical head 的记录数
 */
public record RollbackSummary(int undoRecordsApplied, int recoveryUnavailableRecordsSkipped) {

    public RollbackSummary {
        if (undoRecordsApplied < 0 || recoveryUnavailableRecordsSkipped < 0) {
            throw new DatabaseValidationException("rollback summary counts must not be negative");
        }
    }

    /** 兼容既有 live rollback 调用点；普通路径不存在恢复隔离跳过。 */
    public RollbackSummary(int undoRecordsApplied) {
        this(undoRecordsApplied, 0);
    }
}
