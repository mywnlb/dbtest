package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 列标识（表内序号包装）。
 *
 * @param value 列序号，非负。
 */
public record ColumnId(int value) {

    public ColumnId {
        if (value < 0) {
            throw new DatabaseValidationException("column id must be non-negative: " + value);
        }
    }
}
