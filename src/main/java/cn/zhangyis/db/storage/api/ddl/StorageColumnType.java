package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * DD 到 storage 的列类型 DTO。charset/collation 使用稳定编号，避免 storage.api 向上暴露 record 内部枚举。
 */
public record StorageColumnType(StorageColumnTypeId typeId, boolean nullable, int length, int scale,
                                boolean unsigned, int charsetId, int collationId, List<String> symbols) {
    public StorageColumnType {
        if (typeId == null || length < 0 || scale < 0 || charsetId < 0 || collationId < 0 || symbols == null) {
            throw new DatabaseValidationException("invalid storage column type definition");
        }
        symbols = List.copyOf(symbols);
    }

    /** 创建 BIGINT 类型；完整物理约束由 record mapper 再校验。 */
    public static StorageColumnType bigint(boolean unsigned, boolean nullable) {
        return new StorageColumnType(StorageColumnTypeId.BIGINT, nullable, 0, 0, unsigned, 1, 1, List.of());
    }
}
