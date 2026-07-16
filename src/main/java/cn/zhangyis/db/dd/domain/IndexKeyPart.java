package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 索引 key part；columnId 指向同一 TableDefinition 中的稳定列身份。 */
public record IndexKeyPart(long columnId, IndexOrder order, int prefixBytes) {
    public IndexKeyPart {
        if (columnId <= 0 || order == null || prefixBytes < 0) {
            throw new DatabaseValidationException("index key part has invalid column/order/prefix");
        }
    }
}
