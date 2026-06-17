package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 浮点 codec（FLOAT=4B / DOUBLE=8B，IEEE 754）。**保序编码**（spec §4.4）：取 bits，正数翻符号位、负数翻全部位，
 * 大端存 → 无符号字节序 = 数值序。归一 -0.0→+0.0；NaN 用规范 NaN，排最大（-Inf<负<±0<正<+Inf<NaN）。
 */
public final class FloatingCodec implements TypeCodec {

    private final int width;

    public FloatingCodec(int width) {
        if (width != 4 && width != 8) {
            throw new DatabaseValidationException("floating width must be 4/8: " + width);
        }
        this.width = width;
    }

    @Override
    public int encodedLength(ColumnValue value, ColumnType type) {
        return width;
    }

    @Override
    public int fixedWidth(ColumnType type) {
        return width;
    }

    @Override
    public void validate(ColumnValue value, ColumnType type) {
        if (!(value instanceof ColumnValue.DoubleValue)) {
            throw new InvalidColumnValueException("expected DoubleValue for " + type.typeId());
        }
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        validate(value, type);
        double d = ((ColumnValue.DoubleValue) value).value();
        if (width == 4) {
            int bits = Float.floatToIntBits((float) normalize(d));
            int ordered = bits ^ ((bits >> 31) | Integer.MIN_VALUE);
            for (int i = 3; i >= 0; i--) {
                writer.putByte((ordered >>> (i * 8)) & 0xFF);
            }
        } else {
            long bits = Double.doubleToLongBits(normalize(d));
            long ordered = bits ^ ((bits >> 63) | Long.MIN_VALUE);
            for (int i = 7; i >= 0; i--) {
                writer.putByte((int) ((ordered >>> (i * 8)) & 0xFF));
            }
        }
    }

    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        if (width == 4) {
            int ordered = 0;
            for (int i = 0; i < 4; i++) {
                ordered = (ordered << 8) | slice.byteAt(i);
            }
            int bits = (ordered < 0) ? (ordered ^ Integer.MIN_VALUE) : ~ordered;
            return new ColumnValue.DoubleValue(Float.intBitsToFloat(bits));
        }
        long ordered = 0;
        for (int i = 0; i < 8; i++) {
            ordered = (ordered << 8) | slice.byteAt(i);
        }
        long bits = (ordered < 0) ? (ordered ^ Long.MIN_VALUE) : ~ordered;
        return new ColumnValue.DoubleValue(Double.longBitsToDouble(bits));
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return FieldSlice.compareUnsigned(left, right);
    }

    /** -0.0 归一为 +0.0（MySQL 语义 -0.0==+0.0）；其余原样（NaN 由 *toBits 规范化）。 */
    private static double normalize(double d) {
        return d == 0.0 ? 0.0 : d;
    }
}
