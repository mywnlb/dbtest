package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * 定点 codec（DECIMAL(p,s)）。**保序定长编码**（spec §4.4）：同列 scale 固定为 s，编码 unscaled=value.setScale(s)
 * 的整数，按列宽 W 两补大端（正补 0x00、负补 0xFF）再翻最高位符号位 → 无符号字节序 = 数值序。
 * W = ceil((p·log2(10)+1)/8)。scale 超 s 或有效数字超 p 抛 {@link ColumnValueOutOfRangeException}。
 */
public final class DecimalCodec implements TypeCodec {

    private final int precision;
    private final int scale;
    private final int width;

    public DecimalCodec(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
        this.width = (int) Math.ceil((precision * (Math.log(10) / Math.log(2)) + 1) / 8.0);
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
        if (!(value instanceof ColumnValue.DecimalValue dv)) {
            throw new InvalidColumnValueException("expected DecimalValue for DECIMAL");
        }
        toUnscaled(dv.value());
    }

    private BigInteger toUnscaled(BigDecimal bd) {
        BigDecimal scaled;
        try {
            scaled = bd.setScale(scale);
        } catch (ArithmeticException e) {
            throw new ColumnValueOutOfRangeException("decimal scale exceeds " + scale + ": " + bd.toPlainString(), e);
        }
        BigInteger unscaled = scaled.unscaledValue();
        BigInteger limit = BigInteger.TEN.pow(precision);
        if (unscaled.abs().compareTo(limit) >= 0) {
            throw new ColumnValueOutOfRangeException("decimal precision exceeds " + precision + ": " + bd.toPlainString());
        }
        return unscaled;
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        BigInteger unscaled = toUnscaled(((ColumnValue.DecimalValue) value).value());
        byte[] tc = unscaled.toByteArray();
        byte[] out = new byte[width];
        Arrays.fill(out, (byte) (unscaled.signum() < 0 ? 0xFF : 0x00));
        System.arraycopy(tc, 0, out, width - tc.length, tc.length);
        out[0] ^= (byte) 0x80;
        writer.putBytes(out);
    }

    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        byte[] b = slice.copyBytes();
        b[0] ^= (byte) 0x80;
        BigInteger unscaled = new BigInteger(b);
        return new ColumnValue.DecimalValue(new BigDecimal(unscaled, scale));
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return FieldSlice.compareUnsigned(left, right);
    }
}
