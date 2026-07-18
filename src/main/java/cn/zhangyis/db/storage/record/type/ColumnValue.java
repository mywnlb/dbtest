package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.storage.record.schema.TypeId;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * 逻辑列值（innodb-record-design §8.4，不可变）。不知道自身位于哪页；二进制值与字符串值拷入拷出防外部篡改。
 * NULL 由 record.format 的 NullBitmap 表达，这里以 {@link NullValue#INSTANCE} 作占位。
 */
public sealed interface ColumnValue
        permits ColumnValue.NullValue, ColumnValue.IntValue, ColumnValue.DoubleValue,
                ColumnValue.DecimalValue, ColumnValue.StringValue, ColumnValue.BinaryValue,
                ColumnValue.TemporalValue, ColumnValue.BitValue, ColumnValue.EnumValue, ColumnValue.SetValue,
                ColumnValue.ExternalValue {

    /** NULL 占位单例。
     *
     * <p>未单独声明 Javadoc 的枚举值语义：</p>
     * <ul>
     *     <li>{@code INSTANCE}：无可变状态的单例值，避免为相同空值语义重复分配对象</li>
     * </ul>
     */
    enum NullValue implements ColumnValue { INSTANCE }

    /** 整数值（signed/unsigned 由列类型决定；unsigned 64 位以原始 long bits 承载）。
     *
     * @param value 由 {@code 构造} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     */
    record IntValue(long value) implements ColumnValue { }

    /** 浮点值（FLOAT 与 DOUBLE 共用，FLOAT 编码时窄化为 float）。
     *
     * @param value 由 {@code 构造} 转换或编码的原始 {@code double} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     */
    record DoubleValue(double value) implements ColumnValue { }

    /** 定点值。
     *
     * @param value 待映射为 SQL 精确数值的任意精度值；不得为 {@code null}，精度和标度必须落在目标列类型允许范围
     */
    record DecimalValue(BigDecimal value) implements ColumnValue {
        public DecimalValue {
            if (value == null) {
                throw new DatabaseValidationException("decimal value must not be null");
            }
        }
    }

    /** 字符串值（UTF-8 编码进物理记录）。
     *
     * @param value 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    record StringValue(String value) implements ColumnValue {
        public StringValue {
            if (value == null) {
                throw new DatabaseValidationException("string value must not be null");
            }
        }
    }

    /** 二进制值（防御性拷贝）。
     *
     * @param value 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    record BinaryValue(byte[] value) implements ColumnValue {
        public BinaryValue {
            if (value == null) {
                throw new DatabaseValidationException("binary value must not be null");
            }
            value = value.clone();
        }

        /** 返回防御性副本，避免外部改动内部状态。 */
        @Override
        public byte[] value() {
            return value.clone();
        }
    }

    /**
     * Canonical bit string 字节；有效 bit 从首字节最高位开始，末字节未使用低位是否清零由 {@link BitCodec} 按列宽校验。
     * @param value 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    record BitValue(byte[] value) implements ColumnValue {
        public BitValue {
            if (value == null) {
                throw new DatabaseValidationException("bit value must not be null");
            }
            value = value.clone();
        }

        /** 返回防御性副本，避免页编码前被调用方改写。 */
        @Override
        public byte[] value() {
            return value.clone();
        }
    }

    /** ENUM 的 1-based schema ordinal；具体上界由列字典校验。
     *
     * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
     */
    record EnumValue(int ordinal) implements ColumnValue { }

    /** SET member bitmap；bit 0 对应 schema 中第一个 symbol，最多使用完整 64 位。
     *
     * @param bitmap 参与 {@code 构造} 的无符号位模式 {@code bitmap}；保留全部原始位，不能按 Java 有符号数值语义截断或重排
     */
    record SetValue(long bitmap) implements ColumnValue { }

    /**
     * 记录内 external LOB envelope 的逻辑投影。inlinePrefix 不是完整值，只允许用于受限 prefix index 与诊断。
     *
     * @param typeId 参与 {@code 构造} 的稳定领域标识 {@code TypeId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param reference LOB 数据或其稳定外部引用；不得为 {@code null}，引用身份、长度与校验信息必须匹配所属记录
     * @param inlinePrefix 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    record ExternalValue(TypeId typeId, LobReference reference, byte[] inlinePrefix) implements ColumnValue {
        public ExternalValue {
            if (typeId == null || reference == null || inlinePrefix == null) {
                throw new DatabaseValidationException("external LOB fields must not be null");
            }
            inlinePrefix = inlinePrefix.clone();
        }

        /** 返回防御性副本，避免索引比较或 record 重编码期间被调用方改写。 */
        @Override
        public byte[] inlinePrefix() {
            return inlinePrefix.clone();
        }

        /** byte[] 按内容参与值对象相等性，避免 decode 后因防御性复制得到伪不等。
         *
         * @param other 待比较对象；允许为 {@code null} 或其他类型，此时按 {@code equals} 契约返回 {@code false}
         * @return 比较对象类型与全部值语义相等时为 {@code true}；对象为 {@code null}、类型不同或任一组件不等时为 {@code false}
         */
        @Override
        public boolean equals(Object other) {
            return other instanceof ExternalValue value
                    && typeId == value.typeId
                    && reference.equals(value.reference)
                    && Arrays.equals(inlinePrefix, value.inlinePrefix);
        }

        /** 与内容相等性一致的稳定 hash。
         *
         * @return 由参与值语义的全部组件计算出的稳定哈希值；与 {@code equals} 相等的对象必须返回相同结果
         */
        @Override
        public int hashCode() {
            int result = 31 * typeId.hashCode() + reference.hashCode();
            return 31 * result + Arrays.hashCode(inlinePrefix);
        }
    }

    /**
     * 时间值的统一 long 载体：DATE=epochDay，TIME=duration millis，DATETIME/TIMESTAMP=epoch millis，YEAR=unsigned year。
     * 具体物理范围由与 {@code kind} 匹配的 TemporalCodec 校验。
     * @param kind 选择 {@code 构造} 分支的 {@code TemporalKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param normalized 参与 {@code 构造} 的无符号位模式 {@code normalized}；保留全部原始位，不能按 Java 有符号数值语义截断或重排
     */
    record TemporalValue(TemporalKind kind, long normalized) implements ColumnValue {
        public TemporalValue {
            if (kind == null) {
                throw new DatabaseValidationException("temporal kind must not be null");
            }
        }
    }
}
