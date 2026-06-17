package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 表结构（不可变）。列按 ordinal 0..n-1 连续有序；记录编解码以此为权威列序与类型来源。
 *
 * @param schemaVersion schema 版本，编解码时校验一致。
 * @param columns       有序列定义（防御性不可变副本）。
 */
public record TableSchema(long schemaVersion, List<ColumnDef> columns) {

    public TableSchema {
        if (columns == null || columns.isEmpty()) {
            throw new DatabaseValidationException("table schema must have at least one column");
        }
        columns = List.copyOf(columns);
        for (int i = 0; i < columns.size(); i++) {
            ColumnDef c = columns.get(i);
            if (c.ordinal() != i || c.id().value() != i) {
                throw new DatabaseValidationException(
                        "column ordinal/id must equal position " + i + ": " + c.name());
            }
        }
    }

    /** 列数。 */
    public int columnCount() {
        return columns.size();
    }

    /** 按 ordinal 取列；越界抛校验异常。 */
    public ColumnDef column(int ordinal) {
        if (ordinal < 0 || ordinal >= columns.size()) {
            throw new DatabaseValidationException("column ordinal out of range: " + ordinal);
        }
        return columns.get(ordinal);
    }
}
