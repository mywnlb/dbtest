package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/** 完整用户 SQL 值；external LOB 在进入此层前必须已 hydrate。 */
public sealed interface SqlValue permits SqlValue.NullValue, SqlValue.IntegerValue,
        SqlValue.FloatingValue, SqlValue.DecimalValue, SqlValue.StringValue, SqlValue.BytesValue,
        SqlValue.TemporalValue, SqlValue.BitValue, SqlValue.EnumValue, SqlValue.SetValue {

    /** SQL NULL 单例。 */
    enum NullValue implements SqlValue { INSTANCE }

    /** 覆盖 signed/unsigned BIGINT 全域的整数投影。 */
    record IntegerValue(BigInteger value) implements SqlValue {
        public IntegerValue {
            if (value == null) throw new DatabaseValidationException("SQL integer must not be null");
        }
    }

    /** FLOAT/DOUBLE 的有限 IEEE 值。 */
    record FloatingValue(double value) implements SqlValue {
        public FloatingValue {
            if (!Double.isFinite(value)) throw new DatabaseValidationException("SQL floating value must be finite");
        }
    }

    /** 精确 DECIMAL 投影。 */
    record DecimalValue(BigDecimal value) implements SqlValue {
        public DecimalValue {
            if (value == null) throw new DatabaseValidationException("SQL decimal must not be null");
        }
    }

    /** 字符、ENUM/JSON 之外的普通字符串投影。 */
    record StringValue(String value) implements SqlValue {
        public StringValue {
            if (value == null) throw new DatabaseValidationException("SQL string must not be null");
        }
    }

    /** BINARY/BLOB 完整字节值；构造和访问均防御性复制。 */
    final class BytesValue implements SqlValue {
        private final byte[] value;
        public BytesValue(byte[] value) {
            if (value == null) throw new DatabaseValidationException("SQL bytes must not be null");
            this.value = Arrays.copyOf(value, value.length);
        }
        public byte[] value() { return Arrays.copyOf(value, value.length); }
        @Override public boolean equals(Object other) {
            return other instanceof BytesValue bytes && Arrays.equals(value, bytes.value);
        }
        @Override public int hashCode() { return Arrays.hashCode(value); }
    }

    /** DATE/TIME/DATETIME/TIMESTAMP/YEAR 的稳定公开投影。 */
    record TemporalValue(TemporalKind kind, long value) implements SqlValue {
        public TemporalValue {
            if (kind == null) throw new DatabaseValidationException("SQL temporal kind must not be null");
        }
    }

    enum TemporalKind { DATE, TIME, DATETIME, TIMESTAMP, YEAR }

    /** canonical 左补零 bit string。 */
    final class BitValue implements SqlValue {
        private final byte[] bytes;
        private final int bitWidth;
        public BitValue(byte[] bytes, int bitWidth) {
            if (bytes == null || bitWidth <= 0 || bytes.length != (bitWidth + 7) / 8) {
                throw new DatabaseValidationException("invalid SQL bit value framing");
            }
            this.bytes = Arrays.copyOf(bytes, bytes.length);
            this.bitWidth = bitWidth;
        }
        public byte[] bytes() { return Arrays.copyOf(bytes, bytes.length); }
        public int bitWidth() { return bitWidth; }
        @Override public boolean equals(Object other) {
            return other instanceof BitValue bit && bitWidth == bit.bitWidth && Arrays.equals(bytes, bit.bytes);
        }
        @Override public int hashCode() { return 31 * bitWidth + Arrays.hashCode(bytes); }
    }

    /** ENUM 声明 symbol 与 1-based ordinal。 */
    record EnumValue(String symbol, int ordinal) implements SqlValue {
        public EnumValue {
            if (symbol == null || ordinal <= 0) throw new DatabaseValidationException("invalid SQL ENUM value");
        }
    }

    /** SET 的声明顺序 symbol 列表与 bitmap。 */
    record SetValue(List<String> symbols, long bitmap) implements SqlValue {
        public SetValue {
            if (symbols == null || symbols.stream().anyMatch(java.util.Objects::isNull)) {
                throw new DatabaseValidationException("invalid SQL SET value");
            }
            symbols = List.copyOf(symbols);
        }
    }
}
