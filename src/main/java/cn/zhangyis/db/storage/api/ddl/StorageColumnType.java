package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * DD 到 storage 的列类型 DTO。charset/collation 使用稳定编号，避免 storage.api 向上暴露 record 内部枚举。
 *
 * @param typeId 参与 {@code 构造} 的稳定领域标识 {@code StorageColumnTypeId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
 * @param length 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 * @param scale 参与 {@code 构造} 的上界或规格值 {@code scale}；必须非负且不能使容量、页数或编码长度计算溢出
 * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
 * @param charsetId 参与 {@code 构造} 的原始数值身份 {@code charsetId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param collationId 参与 {@code 构造} 的原始数值身份 {@code collationId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param symbols 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 */
public record StorageColumnType(StorageColumnTypeId typeId, boolean nullable, int length, int scale,
                                boolean unsigned, int charsetId, int collationId, List<String> symbols) {
    public StorageColumnType {
        if (typeId == null || length < 0 || scale < 0 || charsetId < 0 || collationId < 0 || symbols == null) {
            throw new DatabaseValidationException("invalid storage column type definition");
        }
        symbols = List.copyOf(symbols);
    }

    /** 创建 BIGINT 类型；完整物理约束由 record mapper 再校验。
     *
     * @param unsigned 整数值是否按无符号域解释；{@code true} 使用无符号范围与排序，{@code false} 保留 Java 有符号语义
     * @param nullable 列或表达式是否允许 SQL NULL；{@code true} 允许写入 NULL，{@code false} 时绑定或编码阶段必须拒绝 NULL
     * @return {@code bigint} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static StorageColumnType bigint(boolean unsigned, boolean nullable) {
        return new StorageColumnType(StorageColumnTypeId.BIGINT, nullable, 0, 0, unsigned, 1, 1, List.of());
    }
}
