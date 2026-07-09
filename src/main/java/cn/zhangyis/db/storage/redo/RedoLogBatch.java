package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 一次 MTR commit 产生的 redo 批次。批内记录共享同一个 {@link LogRange#end()} 作为恢复幂等边界；
 * recovery 必须先应用批内所有同页记录，再把该页 pageLSN 推到批次 endLsn。
 *
 * @param range   批次占用的 LSN 区间。
 * @param records 批内 redo 记录，按 MTR 收集顺序排列；可同时包含物理页 record 与少量逻辑 intent record。
 */
public record RedoLogBatch(LogRange range, List<RedoRecord> records) {

    public RedoLogBatch {
        if (range == null || records == null) {
            throw new DatabaseValidationException("redo log batch range/records must not be null");
        }
        for (RedoRecord r : records) {
            if (r == null) {
                throw new DatabaseValidationException("redo log batch record must not be null");
            }
        }
        records = List.copyOf(records);
        long expectedEnd = range.start().value();
        for (RedoRecord r : records) {
            int len = r.byteLength();
            if (Long.MAX_VALUE - expectedEnd < len) {
                throw new DatabaseValidationException("redo log batch byte length overflows LSN range");
            }
            expectedEnd += len;
        }
        if (expectedEnd != range.end().value()) {
            throw new DatabaseValidationException("redo log batch range does not match record length: start="
                    + range.start().value() + " end=" + range.end().value() + " expectedEnd=" + expectedEnd);
        }
    }
}
