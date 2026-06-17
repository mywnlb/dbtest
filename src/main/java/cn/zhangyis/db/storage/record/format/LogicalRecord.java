package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.List;

/**
 * 逻辑记录（innodb-record-design §5.3）。列值按 schema ordinal 有序；用户列不含隐藏列。
 *
 * <p>{@code hiddenColumns} 仅聚簇 conventional 记录在场（DB_TRX_ID + DB_ROLL_PTR），非聚簇记录为 {@code null}。
 * 本类**只校验自身形状**（{@link HiddenColumns} 构造器保证在场时两字段非空）；clustered ⇔ hiddenColumns 在场的
 * 一致性由持有 schema 的 {@code RecordEncoder}/{@code RecordFieldResolver} 校验（本类不持 schema，无从判断）。
 * 关键：隐藏列**不进入** {@code columnValues}，避免 materialize 把 MVCC 元数据泄漏给上层。
 *
 * @param schemaVersion 对应 schema 版本（编码时校验一致）。
 * @param columnValues  用户列值（按 ordinal 0..n-1；NULL 用 ColumnValue.NullValue）。
 * @param deleted       delete-mark。
 * @param recordType    记录类型。
 * @param hiddenColumns 聚簇记录隐藏列；非聚簇为 {@code null}。
 */
public record LogicalRecord(long schemaVersion, List<ColumnValue> columnValues, boolean deleted,
                            RecordType recordType, HiddenColumns hiddenColumns) {

    public LogicalRecord {
        if (columnValues == null) {
            throw new DatabaseValidationException("column values must not be null");
        }
        if (recordType == null) {
            throw new DatabaseValidationException("record type must not be null");
        }
        columnValues = List.copyOf(columnValues);
    }

    /** 兼容副构造器：无隐藏列（非聚簇记录）。保留既有四参调用点源码不破。 */
    public LogicalRecord(long schemaVersion, List<ColumnValue> columnValues, boolean deleted,
                         RecordType recordType) {
        this(schemaVersion, columnValues, deleted, recordType, null);
    }
}
