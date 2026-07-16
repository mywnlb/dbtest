package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** DDL 交给 storage 的有序列定义；columnId 是 DD 稳定身份，ordinal 决定物理 record 位置。 */
public record StorageColumnDefinition(long columnId, String name, int ordinal, StorageColumnType type) {
    public StorageColumnDefinition {
        if (columnId <= 0 || name == null || name.isBlank() || ordinal < 0 || type == null) {
            throw new DatabaseValidationException("invalid storage column definition");
        }
    }
}
