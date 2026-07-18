package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 类型编码策略（innodb-record-design §8.3，Strategy）。NULL 不由 codec 处理（由 record.format 的 NullBitmap）。
 * 数值/时间/binary 使用保序编码直接比较；字符类型由 {@link CollationStrategy} 解释编码切片。
 */
public interface TypeCodec {

    /** 该值编码后的字节长度（定长类型与值无关）。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code encodedLength} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code encodedLength} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    int encodedLength(ColumnValue value, ColumnType type);

    /** 定长类型的固定编码宽度（仅由 ColumnType 决定，供解码定长列定位）；变长类型抛 DatabaseValidationException。
     *
     * @param type 选择 {@code fixedWidth} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code fixedWidth} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    int fixedWidth(ColumnType type);

    /** 把值编码写入 writer（要求值已 validate）。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code encode} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param writer 由组合根提供的 {@code FieldWriter} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code encode} 调用
     */
    void encode(ColumnValue value, ColumnType type, FieldWriter writer);

    /** 从切片解码为列值。
     *
     * @param slice 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code decode} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code decode} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    ColumnValue decode(FieldSlice slice, ColumnType type);

    /** 比较两个已编码切片，返回 &lt;0/0/&gt;0；字符类型服从 ColumnType 声明的 collation。
     *
     * @param left 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param right 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code compare} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     */
    int compare(FieldSlice left, FieldSlice right, ColumnType type);

    /**
     * 比较索引 key part。普通类型复用既有 byte-prefix 后的 compare；LOB codec 可解释 inline/external envelope，
     * JSON 等不可索引类型也在这一稳定扩展点 fail-closed。
     *
     * @param left 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param right 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code compareKeyPart} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param prefixBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     */
    default int compareKeyPart(FieldSlice left, FieldSlice right, ColumnType type, int prefixBytes) {
        return compare(KeyPrefix.apply(left, type, prefixBytes), KeyPrefix.apply(right, type, prefixBytes), type);
    }

    /** 校验值与类型相容、长度/范围合法；不合法抛领域异常。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code validate} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     */
    void validate(ColumnValue value, ColumnType type);
}
