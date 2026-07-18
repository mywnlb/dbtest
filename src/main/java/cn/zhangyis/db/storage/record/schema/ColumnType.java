package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 列类型描述符（innodb-record-design §8.1，不可变）。只描述类型属性，不读写页。
 * length 语义：CHAR/VARCHAR/BINARY/VARBINARY = 最大字节长度；DECIMAL = 精度 p；BIT = bit width；
 * ENUM/SET = symbols 数量；TEXT/BLOB/JSON = 最大 payload 字节数；其余 = 0。
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
 * @param symbols     ENUM/SET 的不可变声明顺序；其它类型必须为空。
 */
public record ColumnType(TypeId typeId, boolean nullable, int length, int scale, boolean unsigned,
                         CharsetId charset, CollationId collation, StorageKind storageKind,
                         List<String> symbols) {

    /** DECIMAL 精度上限（教学简化）。 */
    public static final int MAX_DECIMAL_PRECISION = 38;

    public ColumnType {
        if (typeId == null || charset == null || collation == null || storageKind == null || symbols == null) {
            throw new DatabaseValidationException("column type enum fields must not be null");
        }
        Set<String> uniqueSymbols = new HashSet<>();
        for (String symbol : symbols) {
            if (symbol == null || symbol.isBlank()) {
                throw new DatabaseValidationException("ENUM/SET symbols must not be null or blank");
            }
            if (!uniqueSymbols.add(symbol)) {
                throw new DatabaseValidationException("duplicate ENUM/SET symbol: " + symbol);
            }
        }
        symbols = List.copyOf(symbols);
        switch (typeId) {
            case DECIMAL -> {
                if (length < 1 || length > MAX_DECIMAL_PRECISION) {
                    throw new DatabaseValidationException("decimal precision out of range: " + length);
                }
                if (scale < 0 || scale > length) {
                    throw new DatabaseValidationException("decimal scale out of range: " + scale);
                }
            }
            case CHAR, VARCHAR, BINARY, VARBINARY,
                    TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
                    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> {
                if (length < 1) {
                    throw new DatabaseValidationException("char/binary length must be positive: " + length);
                }
                if (scale != 0) {
                    throw new DatabaseValidationException("scale only valid for DECIMAL");
                }
            }
            case BIT -> {
                if (length < 1 || length > 64) {
                    throw new DatabaseValidationException("BIT width out of range 1..64: " + length);
                }
                if (scale != 0) {
                    throw new DatabaseValidationException("scale only valid for DECIMAL");
                }
            }
            case ENUM -> validateSymbols(typeId, length, scale, symbols, 65_535);
            case SET -> validateSymbols(typeId, length, scale, symbols, 64);
            default -> {
                if (length != 0) {
                    throw new DatabaseValidationException("length only valid for DECIMAL/char/binary types");
                }
                if (scale != 0) {
                    throw new DatabaseValidationException("scale only valid for DECIMAL");
                }
            }
        }
        if (typeId != TypeId.ENUM && typeId != TypeId.SET && !symbols.isEmpty()) {
            throw new DatabaseValidationException("symbols only valid for ENUM/SET: " + typeId);
        }
        if (unsigned && typeId != TypeId.TINYINT && typeId != TypeId.SMALLINT
                && typeId != TypeId.INT && typeId != TypeId.BIGINT) {
            throw new DatabaseValidationException("unsigned flag only valid for integer types: " + typeId);
        }
        StorageKind expected = switch (typeId) {
            case VARCHAR, VARBINARY -> StorageKind.VARIABLE;
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
                    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> StorageKind.OVERFLOW_CAPABLE;
            default -> StorageKind.FIXED;
        };
        if (storageKind != expected) {
            throw new DatabaseValidationException("storageKind mismatch for " + typeId + ": " + storageKind);
        }
    }

    private static ColumnType fixed(TypeId id, int length, int scale, boolean unsigned, boolean nullable) {
        return new ColumnType(id, nullable, length, scale, unsigned, CharsetId.UTF8, CollationId.BINARY,
                StorageKind.FIXED, List.of());
    }

    /** 校验 schema-owned 字典规模与 length 一致；声明顺序是 ordinal/bitmap 的权威。 */
    private static void validateSymbols(TypeId typeId, int length, int scale, List<String> symbols, int max) {
        if (symbols.isEmpty() || symbols.size() > max || length != symbols.size()) {
            throw new DatabaseValidationException(
                    typeId + " symbol count must be 1.." + max + " and equal length: " + symbols.size());
        }
        if (scale != 0) {
            throw new DatabaseValidationException("scale only valid for DECIMAL");
        }
    }

    /**
     * 根据调用参数创建或转换 {@code tinyint} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code tinyint} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType tinyint(boolean unsigned, boolean nullable) {
        return fixed(TypeId.TINYINT, 0, 0, unsigned, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code smallint} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code smallint} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType smallint(boolean unsigned, boolean nullable) {
        return fixed(TypeId.SMALLINT, 0, 0, unsigned, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code intType} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code intType} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType intType(boolean unsigned, boolean nullable) {
        return fixed(TypeId.INT, 0, 0, unsigned, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code bigint} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code bigint} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType bigint(boolean unsigned, boolean nullable) {
        return fixed(TypeId.BIGINT, 0, 0, unsigned, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code floatType} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code floatType} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType floatType(boolean nullable) {
        return fixed(TypeId.FLOAT, 0, 0, false, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code doubleType} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code doubleType} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType doubleType(boolean nullable) {
        return fixed(TypeId.DOUBLE, 0, 0, false, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code decimal} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param precision 参与 {@code decimal} 的上界或规格值 {@code precision}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param scale 参与 {@code decimal} 的上界或规格值 {@code scale}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code decimal} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType decimal(int precision, int scale, boolean nullable) {
        return fixed(TypeId.DECIMAL, precision, scale, false, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code charType} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param nBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code charType} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType charType(int nBytes, boolean nullable) {
        return charType(nBytes, nullable, CharsetId.UTF8, CollationId.BINARY);
    }

    /**
     * 创建显式字符集与排序规则的 CHAR；pair 的生产支持性由只读类型 registry 在 codec 选择时复核。
     *
     * @param nBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @param charset 参与 {@code charType} 的稳定领域标识 {@code CharsetId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param collation 参与 {@code charType} 的稳定领域标识 {@code CollationId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code charType} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType charType(int nBytes, boolean nullable, CharsetId charset, CollationId collation) {
        return new ColumnType(TypeId.CHAR, nullable, nBytes, 0, false, charset, collation,
                StorageKind.FIXED, List.of());
    }

    /**
     * 根据调用参数创建或转换 {@code binary} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param nBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code binary} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType binary(int nBytes, boolean nullable) {
        return fixed(TypeId.BINARY, nBytes, 0, false, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code date} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code date} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType date(boolean nullable) {
        return fixed(TypeId.DATE, 0, 0, false, nullable);
    }

    /** 创建 8B 带符号毫秒 duration 类型；MySQL TIME 业务范围留给 SQL 层校验。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code time} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType time(boolean nullable) {
        return fixed(TypeId.TIME, 0, 0, false, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code datetime} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code datetime} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType datetime(boolean nullable) {
        return fixed(TypeId.DATETIME, 0, 0, false, nullable);
    }

    /** 创建 8B UTC epoch millis 类型；session 时区转换不能进入 Record 层。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code timestamp} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType timestamp(boolean nullable) {
        return fixed(TypeId.TIMESTAMP, 0, 0, false, nullable);
    }

    /** 创建 2B unsigned 教学年份类型；物理可表达范围由 TemporalCodec 校验。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code year} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType year(boolean nullable) {
        return fixed(TypeId.YEAR, 0, 0, false, nullable);
    }

    /** 创建 1..64 位定长 bit string；length 表示 bit 数而非编码 byte 数。
     *
     * @param bitWidth 参与 {@code bit} 的原始数值身份 {@code bitWidth}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code bit} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType bit(int bitWidth, boolean nullable) {
        return fixed(TypeId.BIT, bitWidth, 0, false, nullable);
    }

    /** 创建声明顺序固定的 ENUM；逻辑值使用 1-based ordinal。
     *
     * @param symbols 参与 {@code enumType} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code enumType} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType enumType(List<String> symbols, boolean nullable) {
        int count = symbols == null ? 0 : symbols.size();
        return new ColumnType(TypeId.ENUM, nullable, count, 0, false,
                CharsetId.UTF8, CollationId.BINARY, StorageKind.FIXED, symbols);
    }

    /** 创建最多 64 个 member 的 SET；逻辑值使用 ordinal 对应 bit 的 bitmap。
     *
     * @param symbols 参与 {@code setType} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code setType} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType setType(List<String> symbols, boolean nullable) {
        int count = symbols == null ? 0 : symbols.size();
        return new ColumnType(TypeId.SET, nullable, count, 0, false,
                CharsetId.UTF8, CollationId.BINARY, StorageKind.FIXED, symbols);
    }

    /**
     * 根据调用参数创建或转换 {@code varchar} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param nBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code varchar} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType varchar(int nBytes, boolean nullable) {
        return varchar(nBytes, nullable, CharsetId.UTF8, CollationId.BINARY);
    }

    /**
     * 创建显式字符集与排序规则的 VARCHAR；长度继续按编码后最大字节数解释，不按字符数解释。
     *
     * @param nBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @param charset 参与 {@code varchar} 的稳定领域标识 {@code CharsetId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param collation 参与 {@code varchar} 的稳定领域标识 {@code CollationId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code varchar} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType varchar(int nBytes, boolean nullable, CharsetId charset, CollationId collation) {
        return new ColumnType(TypeId.VARCHAR, nullable, nBytes, 0, false, charset, collation,
                StorageKind.VARIABLE, List.of());
    }

    /**
     * 根据调用参数创建或转换 {@code varbinary} 返回的 {@code ColumnType}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param nBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code varbinary} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType varbinary(int nBytes, boolean nullable) {
        return new ColumnType(TypeId.VARBINARY, nullable, nBytes, 0, false, CharsetId.UTF8, CollationId.BINARY,
                StorageKind.VARIABLE, List.of());
    }

    /** 创建 UTF-8/BINARY TINYTEXT（最大 255B）。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code tinyText} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType tinyText(boolean nullable) {
        return textType(TypeId.TINYTEXT, 255, nullable, CharsetId.UTF8, CollationId.BINARY);
    }

    /** 创建 UTF-8/BINARY TEXT（最大 65535B）。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code text} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType text(boolean nullable) {
        return textType(TypeId.TEXT, 65_535, nullable, CharsetId.UTF8, CollationId.BINARY);
    }

    /** 创建显式字符语义的 TEXT。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @param charset 参与 {@code text} 的稳定领域标识 {@code CharsetId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param collation 参与 {@code text} 的稳定领域标识 {@code CollationId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code text} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType text(boolean nullable, CharsetId charset, CollationId collation) {
        return textType(TypeId.TEXT, 65_535, nullable, charset, collation);
    }

    /** 创建 UTF-8/BINARY MEDIUMTEXT（最大 16777215B）。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code mediumText} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType mediumText(boolean nullable) {
        return textType(TypeId.MEDIUMTEXT, 16_777_215, nullable, CharsetId.UTF8, CollationId.BINARY);
    }

    /** 创建 UTF-8/BINARY LONGTEXT；Java 数组边界使 v1 封顶 Integer.MAX_VALUE。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code longText} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType longText(boolean nullable) {
        return textType(TypeId.LONGTEXT, Integer.MAX_VALUE, nullable, CharsetId.UTF8, CollationId.BINARY);
    }

    /** 创建 TINYBLOB（最大 255B）。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code tinyBlob} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType tinyBlob(boolean nullable) {
        return binaryLob(TypeId.TINYBLOB, 255, nullable);
    }

    /** 创建 BLOB（最大 65535B）。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code blob} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType blob(boolean nullable) {
        return binaryLob(TypeId.BLOB, 65_535, nullable);
    }

    /** 创建 MEDIUMBLOB（最大 16777215B）。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code mediumBlob} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType mediumBlob(boolean nullable) {
        return binaryLob(TypeId.MEDIUMBLOB, 16_777_215, nullable);
    }

    /** 创建 LONGBLOB；Java 数组边界使 v1 封顶 Integer.MAX_VALUE。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code longBlob} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType longBlob(boolean nullable) {
        return binaryLob(TypeId.LONGBLOB, Integer.MAX_VALUE, nullable);
    }

    /** 创建 v1 JSON：严格 UTF-8 文本、不可进入核心索引比较。
     *
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code json} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static ColumnType json(boolean nullable) {
        return textType(TypeId.JSON, Integer.MAX_VALUE, nullable, CharsetId.UTF8, CollationId.BINARY);
    }

    private static ColumnType textType(TypeId typeId, int maxBytes, boolean nullable,
                                       CharsetId charset, CollationId collation) {
        return new ColumnType(typeId, nullable, maxBytes, 0, false, charset, collation,
                StorageKind.OVERFLOW_CAPABLE, List.of());
    }

    private static ColumnType binaryLob(TypeId typeId, int maxBytes, boolean nullable) {
        return new ColumnType(typeId, nullable, maxBytes, 0, false, CharsetId.UTF8, CollationId.BINARY,
                StorageKind.OVERFLOW_CAPABLE, List.of());
    }
}
