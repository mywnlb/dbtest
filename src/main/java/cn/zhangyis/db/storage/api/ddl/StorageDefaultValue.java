package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * SQL Binder 到 storage rebuild 的稳定常量 DTO。它只表达已经按目标 DD 类型验证的值，不携带 SQL AST，
 * storage 将其映射为 Record {@code ColumnValue}；byte[] 构造与访问均防御性复制。
 */
public sealed interface StorageDefaultValue permits StorageDefaultValue.NullValue,
        StorageDefaultValue.IntegerValue, StorageDefaultValue.FloatingValue,
        StorageDefaultValue.DecimalValue, StorageDefaultValue.StringValue,
        StorageDefaultValue.BytesValue, StorageDefaultValue.TemporalValue,
        StorageDefaultValue.BitValue, StorageDefaultValue.EnumValue,
        StorageDefaultValue.SetValue {

    /** SQL NULL 常量。 */
    enum NullValue implements StorageDefaultValue {
        INSTANCE
    }

    /** @param value 已完成 signed/unsigned 范围校验的整数 */
    record IntegerValue(BigInteger value) implements StorageDefaultValue {
        public IntegerValue {
            if (value == null) {
                throw new DatabaseValidationException("storage default integer must not be null");
            }
        }
    }

    /** @param value 有限浮点值 */
    record FloatingValue(double value) implements StorageDefaultValue {
        public FloatingValue {
            if (!Double.isFinite(value)) {
                throw new DatabaseValidationException("storage default floating value must be finite");
            }
        }
    }

    /** @param value 精确 DECIMAL 值 */
    record DecimalValue(BigDecimal value) implements StorageDefaultValue {
        public DecimalValue {
            if (value == null) {
                throw new DatabaseValidationException("storage default decimal must not be null");
            }
        }
    }

    /** @param value 字符/JSON 值 */
    record StringValue(String value) implements StorageDefaultValue {
        public StringValue {
            if (value == null) {
                throw new DatabaseValidationException("storage default string must not be null");
            }
        }
    }

    /** 完整 binary/blob bytes。 */
    final class BytesValue implements StorageDefaultValue {
        private final byte[] value;

        public BytesValue(byte[] value) {
            if (value == null) {
                throw new DatabaseValidationException("storage default bytes must not be null");
            }
            this.value = value.clone();
        }

        public byte[] value() {
            return value.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof BytesValue bytes && Arrays.equals(value, bytes.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }

    /** @param kind 时间类别 @param value 归一化 long */
    record TemporalValue(TemporalKind kind, long value) implements StorageDefaultValue {
        public TemporalValue {
            if (kind == null) {
                throw new DatabaseValidationException("storage default temporal kind must not be null");
            }
        }
    }

    /** BIT canonical bytes。 */
    final class BitValue implements StorageDefaultValue {
        private final byte[] value;

        public BitValue(byte[] value) {
            if (value == null || value.length == 0) {
                throw new DatabaseValidationException("storage default bit bytes must not be empty");
            }
            this.value = value.clone();
        }

        public byte[] value() {
            return value.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof BitValue bits && Arrays.equals(value, bits.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }

    /** @param ordinal ENUM 1-based ordinal */
    record EnumValue(int ordinal) implements StorageDefaultValue {
        public EnumValue {
            if (ordinal <= 0) {
                throw new DatabaseValidationException("storage default ENUM ordinal must be positive");
            }
        }
    }

    /** @param bitmap SET 成员位图 */
    record SetValue(long bitmap) implements StorageDefaultValue {
    }

    /** 时间常量类别，与 record TemporalKind 显式映射。 */
    enum TemporalKind {
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        YEAR
    }
}
