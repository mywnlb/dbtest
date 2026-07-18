package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * SQL/DD 可持久化列类型描述。charset/collation 使用稳定数值 ID，只有 storage adapter 了解 Record 层枚举；
 * symbols 顺序是 ENUM/SET ordinal/bitmap 的权威。
 *
 * @param typeId 参与 {@code 构造} 的稳定领域标识 {@code DictionaryTypeId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
 * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
 * @param length 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 * @param scale 参与 {@code 构造} 的上界或规格值 {@code scale}；必须非负且不能使容量、页数或编码长度计算溢出
 * @param charsetId 参与 {@code 构造} 的原始数值身份 {@code charsetId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param collationId 参与 {@code 构造} 的原始数值身份 {@code collationId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param symbols 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
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

    /**
     * 根据调用参数创建或转换 {@code integer} 返回的 {@code ColumnTypeDefinition}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code integer} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     */
    public static ColumnTypeDefinition integer(boolean unsigned, boolean nullable) {
        return scalar(DictionaryTypeId.INT, unsigned, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code bigint} 返回的 {@code ColumnTypeDefinition}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code bigint} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     */
    public static ColumnTypeDefinition bigint(boolean unsigned, boolean nullable) {
        return scalar(DictionaryTypeId.BIGINT, unsigned, nullable);
    }

    /**
     * 根据调用参数创建或转换 {@code scalar} 返回的 {@code ColumnTypeDefinition}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param typeId 参与 {@code scalar} 的稳定领域标识 {@code DictionaryTypeId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code scalar} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     */
    public static ColumnTypeDefinition scalar(DictionaryTypeId typeId, boolean unsigned, boolean nullable) {
        return new ColumnTypeDefinition(typeId, unsigned, nullable, 0, 0, 0, 0, List.of());
    }
}
