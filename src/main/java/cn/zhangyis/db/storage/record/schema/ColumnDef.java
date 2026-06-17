package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 列定义。
 *
 * @param id      列标识。
 * @param name    列名（非空白）。
 * @param type    列类型。
 * @param ordinal 表内有序位置（0..n-1）。
 */
public record ColumnDef(ColumnId id, String name, ColumnType type, int ordinal) {

    public ColumnDef {
        if (id == null || type == null) {
            throw new DatabaseValidationException("column def id/type must not be null");
        }
        if (name == null || name.isBlank()) {
            throw new DatabaseValidationException("column name must not be blank");
        }
        if (ordinal < 0) {
            throw new DatabaseValidationException("column ordinal must be non-negative: " + ordinal);
        }
    }
}
