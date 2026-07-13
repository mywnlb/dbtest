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

    /** NULL 占位单例。 */
    enum NullValue implements ColumnValue { INSTANCE }

    /** 整数值（signed/unsigned 由列类型决定；unsigned 64 位以原始 long bits 承载）。 */
    record IntValue(long value) implements ColumnValue { }

    /** 浮点值（FLOAT 与 DOUBLE 共用，FLOAT 编码时窄化为 float）。 */
    record DoubleValue(double value) implements ColumnValue { }

    /** 定点值。 */
    record DecimalValue(BigDecimal value) implements ColumnValue {
        public DecimalValue {
            if (value == null) {
                throw new DatabaseValidationException("decimal value must not be null");
            }
        }
    }

    /** 字符串值（UTF-8 编码进物理记录）。 */
    record StringValue(String value) implements ColumnValue {
        public StringValue {
            if (value == null) {
                throw new DatabaseValidationException("string value must not be null");
            }
        }
    }

    /** 二进制值（防御性拷贝）。 */
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

    /** ENUM 的 1-based schema ordinal；具体上界由列字典校验。 */
    record EnumValue(int ordinal) implements ColumnValue { }

    /** SET member bitmap；bit 0 对应 schema 中第一个 symbol，最多使用完整 64 位。 */
    record SetValue(long bitmap) implements ColumnValue { }

    /**
     * 记录内 external LOB envelope 的逻辑投影。inlinePrefix 不是完整值，只允许用于受限 prefix index 与诊断。
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

        /** byte[] 按内容参与值对象相等性，避免 decode 后因防御性复制得到伪不等。 */
        @Override
        public boolean equals(Object other) {
            return other instanceof ExternalValue value
                    && typeId == value.typeId
                    && reference.equals(value.reference)
                    && Arrays.equals(inlinePrefix, value.inlinePrefix);
        }

        /** 与内容相等性一致的稳定 hash。 */
        @Override
        public int hashCode() {
            int result = 31 * typeId.hashCode() + reference.hashCode();
            return 31 * result + Arrays.hashCode(inlinePrefix);
        }
    }

    /**
     * 时间值的统一 long 载体：DATE=epochDay，TIME=duration millis，DATETIME/TIMESTAMP=epoch millis，YEAR=unsigned year。
     * 具体物理范围由与 {@code kind} 匹配的 TemporalCodec 校验。
     */
    record TemporalValue(TemporalKind kind, long normalized) implements ColumnValue {
        public TemporalValue {
            if (kind == null) {
                throw new DatabaseValidationException("temporal kind must not be null");
            }
        }
    }
}
