package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 类型编码策略（innodb-record-design §8.3，Strategy）。NULL 不由 codec 处理（由 record.format 的 NullBitmap）。
 * 数值/时间/binary 使用保序编码直接比较；字符类型由 {@link CollationStrategy} 解释编码切片。
 */
public interface TypeCodec {

    /** 该值编码后的字节长度（定长类型与值无关）。 */
    int encodedLength(ColumnValue value, ColumnType type);

    /** 定长类型的固定编码宽度（仅由 ColumnType 决定，供解码定长列定位）；变长类型抛 DatabaseValidationException。 */
    int fixedWidth(ColumnType type);

    /** 把值编码写入 writer（要求值已 validate）。 */
    void encode(ColumnValue value, ColumnType type, FieldWriter writer);

    /** 从切片解码为列值。 */
    ColumnValue decode(FieldSlice slice, ColumnType type);

    /** 比较两个已编码切片，返回 &lt;0/0/&gt;0；字符类型服从 ColumnType 声明的 collation。 */
    int compare(FieldSlice left, FieldSlice right, ColumnType type);

    /** 校验值与类型相容、长度/范围合法；不合法抛领域异常。 */
    void validate(ColumnValue value, ColumnType type);
}
