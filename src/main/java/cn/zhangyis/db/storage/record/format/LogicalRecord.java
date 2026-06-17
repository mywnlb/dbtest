package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.List;

/**
 * 逻辑记录（innodb-record-design §5.3，本片不含隐藏列）。列值按 schema ordinal 有序。
 *
 * @param schemaVersion 对应 schema 版本（编码时校验一致）。
 * @param columnValues  列值（按 ordinal 0..n-1；NULL 用 ColumnValue.NullValue）。
 * @param deleted       delete-mark。
 * @param recordType    记录类型。
 */
public record LogicalRecord(long schemaVersion, List<ColumnValue> columnValues, boolean deleted,
                            RecordType recordType) {

    public LogicalRecord {
        if (columnValues == null) {
            throw new DatabaseValidationException("column values must not be null");
        }
        if (recordType == null) {
            throw new DatabaseValidationException("record type must not be null");
        }
        columnValues = List.copyOf(columnValues);
    }
}
