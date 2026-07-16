package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * SQL/DD 可持久化列类型描述。charset/collation 使用稳定数值 ID，只有 storage adapter 了解 Record 层枚举；
 * symbols 顺序是 ENUM/SET ordinal/bitmap 的权威。
 */
public record ColumnTypeDefinition(DictionaryTypeId typeId, boolean unsigned, boolean nullable,
                                   int length, int scale, int charsetId, int collationId,
                                   List<String> symbols) {

    public ColumnTypeDefinition {
        if (typeId == null || symbols == null) {
            throw new DatabaseValidationException("dictionary column type/symbols must not be null");
        }
        if (length < 0 || scale < 0 || charsetId < 0 || collationId < 0) {
            throw new DatabaseValidationException("dictionary column type numeric attributes must be non-negative");
        }
        symbols = List.copyOf(symbols);
    }

    public static ColumnTypeDefinition integer(boolean unsigned, boolean nullable) {
        return scalar(DictionaryTypeId.INT, unsigned, nullable);
    }

    public static ColumnTypeDefinition bigint(boolean unsigned, boolean nullable) {
        return scalar(DictionaryTypeId.BIGINT, unsigned, nullable);
    }

    public static ColumnTypeDefinition scalar(DictionaryTypeId typeId, boolean unsigned, boolean nullable) {
        return new ColumnTypeDefinition(typeId, unsigned, nullable, 0, 0, 0, 0, List.of());
    }
}
