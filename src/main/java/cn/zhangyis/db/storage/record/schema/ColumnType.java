package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 列类型描述符（innodb-record-design §8.1，不可变）。只描述类型属性，不读写页。
 * length 语义：CHAR/VARCHAR/BINARY/VARBINARY = 最大字节长度；DECIMAL = 精度 p；其余 = 0。
 * scale 仅 DECIMAL 用；unsigned 仅整数用；charset/collation 仅字符类型有效（其余携带默认值，被忽略）。
 *
 * @param typeId      类型。
 * @param nullable    是否允许 NULL。
 * @param length      见上。
 * @param scale       DECIMAL 小数位。
 * @param unsigned    整数是否无符号。
 * @param charset     字符集。
 * @param collation   排序规则。
 * @param storageKind 定长/变长。
 */
public record ColumnType(TypeId typeId, boolean nullable, int length, int scale, boolean unsigned,
                         CharsetId charset, CollationId collation, StorageKind storageKind) {

    /** DECIMAL 精度上限（教学简化）。 */
    public static final int MAX_DECIMAL_PRECISION = 38;

    public ColumnType {
        if (typeId == null || charset == null || collation == null || storageKind == null) {
            throw new DatabaseValidationException("column type enum fields must not be null");
        }
        switch (typeId) {
            case DECIMAL -> {
                if (length < 1 || length > MAX_DECIMAL_PRECISION) {
                    throw new DatabaseValidationException("decimal precision out of range: " + length);
                }
                if (scale < 0 || scale > length) {
                    throw new DatabaseValidationException("decimal scale out of range: " + scale);
                }
            }
            case CHAR, VARCHAR, BINARY, VARBINARY -> {
                if (length < 1) {
                    throw new DatabaseValidationException("char/binary length must be positive: " + length);
                }
                if (scale != 0) {
                    throw new DatabaseValidationException("scale only valid for DECIMAL");
                }
            }
            default -> {
                if (scale != 0) {
                    throw new DatabaseValidationException("scale only valid for DECIMAL");
                }
            }
        }
        StorageKind expected = (typeId == TypeId.VARCHAR || typeId == TypeId.VARBINARY)
                ? StorageKind.VARIABLE : StorageKind.FIXED;
        if (storageKind != expected) {
            throw new DatabaseValidationException("storageKind mismatch for " + typeId + ": " + storageKind);
        }
    }

    private static ColumnType fixed(TypeId id, int length, int scale, boolean unsigned, boolean nullable) {
        return new ColumnType(id, nullable, length, scale, unsigned, CharsetId.UTF8, CollationId.BINARY,
                StorageKind.FIXED);
    }

    public static ColumnType tinyint(boolean unsigned, boolean nullable) {
        return fixed(TypeId.TINYINT, 0, 0, unsigned, nullable);
    }

    public static ColumnType smallint(boolean unsigned, boolean nullable) {
        return fixed(TypeId.SMALLINT, 0, 0, unsigned, nullable);
    }

    public static ColumnType intType(boolean unsigned, boolean nullable) {
        return fixed(TypeId.INT, 0, 0, unsigned, nullable);
    }

    public static ColumnType bigint(boolean unsigned, boolean nullable) {
        return fixed(TypeId.BIGINT, 0, 0, unsigned, nullable);
    }

    public static ColumnType floatType(boolean nullable) {
        return fixed(TypeId.FLOAT, 0, 0, false, nullable);
    }

    public static ColumnType doubleType(boolean nullable) {
        return fixed(TypeId.DOUBLE, 0, 0, false, nullable);
    }

    public static ColumnType decimal(int precision, int scale, boolean nullable) {
        return fixed(TypeId.DECIMAL, precision, scale, false, nullable);
    }

    public static ColumnType charType(int nBytes, boolean nullable) {
        return fixed(TypeId.CHAR, nBytes, 0, false, nullable);
    }

    public static ColumnType binary(int nBytes, boolean nullable) {
        return fixed(TypeId.BINARY, nBytes, 0, false, nullable);
    }

    public static ColumnType date(boolean nullable) {
        return fixed(TypeId.DATE, 0, 0, false, nullable);
    }

    public static ColumnType datetime(boolean nullable) {
        return fixed(TypeId.DATETIME, 0, 0, false, nullable);
    }

    public static ColumnType varchar(int nBytes, boolean nullable) {
        return new ColumnType(TypeId.VARCHAR, nullable, nBytes, 0, false, CharsetId.UTF8, CollationId.BINARY,
                StorageKind.VARIABLE);
    }

    public static ColumnType varbinary(int nBytes, boolean nullable) {
        return new ColumnType(TypeId.VARBINARY, nullable, nBytes, 0, false, CharsetId.UTF8, CollationId.BINARY,
                StorageKind.VARIABLE);
    }
}
