package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 复合索引中的一个 key part。
 *
 * @param columnId    参与列。
 * @param order       ASC/DESC。
 * @param prefixBytes 前缀字节数；0 表示整列。
 */
public record KeyPartDef(ColumnId columnId, KeyOrder order, int prefixBytes) {

    public KeyPartDef {
        if (columnId == null || order == null) {
            throw new DatabaseValidationException("key part columnId/order must not be null");
        }
        if (prefixBytes < 0) {
            throw new DatabaseValidationException("prefix bytes must be non-negative: " + prefixBytes);
        }
    }
}
