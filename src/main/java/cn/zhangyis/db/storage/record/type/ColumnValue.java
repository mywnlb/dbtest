package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.math.BigDecimal;

/**
 * 逻辑列值（innodb-record-design §8.4，不可变）。不知道自身位于哪页；二进制值与字符串值拷入拷出防外部篡改。
 * NULL 由 record.format 的 NullBitmap 表达，这里以 {@link NullValue#INSTANCE} 作占位。
 */
public sealed interface ColumnValue
        permits ColumnValue.NullValue, ColumnValue.IntValue, ColumnValue.DoubleValue,
                ColumnValue.DecimalValue, ColumnValue.StringValue, ColumnValue.BinaryValue,
                ColumnValue.TemporalValue {

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

    /** 时间值（归一化为 long：DATE=epochDay、DATETIME=epochMilli）。 */
    record TemporalValue(TemporalKind kind, long normalized) implements ColumnValue {
        public TemporalValue {
            if (kind == null) {
                throw new DatabaseValidationException("temporal kind must not be null");
            }
        }
    }
}
