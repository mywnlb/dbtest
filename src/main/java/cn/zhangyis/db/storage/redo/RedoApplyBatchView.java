package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * redo apply 内部视图：保留原始 {@link LogRange}，但允许 records 是按恢复策略过滤后的子集。
 *
 * <p>{@link RedoLogBatch} 必须验证 range 长度等于全部 records 的物理长度；FORCE_SKIP_CORRUPT_TABLESPACE
 * 过滤后不能重新构造 RedoLogBatch，否则 batch end LSN 会被错误地改写，破坏 pageLSN 幂等边界。
 *
 * @param range 原始 batch 的 LSN 区间，尤其 {@link LogRange#end()} 仍作为成功重放页的 pageLSN。
 * @param records 本次实际交给 handler 的 redo records，可能是原始 batch 的子集。
 */
record RedoApplyBatchView(LogRange range, List<RedoRecord> records) {

    RedoApplyBatchView {
        if (range == null || records == null) {
            throw new DatabaseValidationException("redo apply batch view range/records must not be null");
        }
        for (RedoRecord record : records) {
            if (record == null) {
                throw new DatabaseValidationException("redo apply batch view record must not be null");
            }
        }
        records = List.copyOf(records);
    }

    /** @return 当前 view 是否没有任何可应用记录。 */
    boolean isEmpty() {
        return records.isEmpty();
    }
}
