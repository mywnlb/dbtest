package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 索引键列引用；columnId 在映射物理 ordinal 前必须能解析到本表列。 */
public record StorageIndexKeyPart(long columnId, StorageIndexOrder order, int prefixBytes) {
    public StorageIndexKeyPart {
        if (columnId <= 0 || order == null || prefixBytes < 0) {
            throw new DatabaseValidationException("invalid storage index key part");
        }
    }
}
