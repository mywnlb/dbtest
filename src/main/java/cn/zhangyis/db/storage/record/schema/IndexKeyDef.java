package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 索引 key 定义：有序 key part 组合。
 *
 * @param indexId 索引标识。
 * @param parts   有序 key part（非空，防御性不可变副本）。
 */
public record IndexKeyDef(long indexId, List<KeyPartDef> parts) {

    public IndexKeyDef {
        if (parts == null || parts.isEmpty()) {
            throw new DatabaseValidationException("index key must have at least one part");
        }
        parts = List.copyOf(parts);
    }
}
