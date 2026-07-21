package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * shadow rebuild 的单个目标列投影。
 *
 * @param sourceOrdinal 复用源列时为非负 ordinal；新增列为 -1
 * @param defaultValue 仅 sourceOrdinal=-1 可存在；REQUIRED 新列允许为空，但只有源表无行时才能成功
 */
public record StorageColumnRewrite(int sourceOrdinal,
                                   Optional<StorageDefaultValue> defaultValue) {
    public StorageColumnRewrite {
        if (sourceOrdinal < -1 || defaultValue == null
                || sourceOrdinal >= 0 && defaultValue.isPresent()) {
            throw new DatabaseValidationException("storage column rewrite shape is invalid");
        }
    }

    /** @return 复用源行指定 ordinal 的投影 */
    public static StorageColumnRewrite source(int ordinal) {
        return new StorageColumnRewrite(ordinal, Optional.empty());
    }

    /** @return 新列常量投影；empty 表示 REQUIRED 且只允许空源表 */
    public static StorageColumnRewrite added(Optional<StorageDefaultValue> value) {
        return new StorageColumnRewrite(-1, value);
    }
}
