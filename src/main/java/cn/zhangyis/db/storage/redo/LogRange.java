package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * 一次 redo append 占据的 LSN 区间 {@code [start, end)}。end 为本批之后第一个空闲 LSN。
 *
 * @param start 区间起始 LSN。
 * @param end   区间结束（开区间）；空批时 end==start。
 */
public record LogRange(Lsn start, Lsn end) {

    public LogRange {
        if (start == null || end == null) {
            throw new DatabaseValidationException("log range start/end must not be null");
        }
        if (end.value() < start.value()) {
            throw new DatabaseValidationException("log range end < start: " + end.value() + " < " + start.value());
        }
    }
}
