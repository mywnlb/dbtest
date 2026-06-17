package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 整数 codec（TINYINT/SMALLINT/INT/BIGINT）。**保序编码**（spec §4.4）：signed 翻最高位符号位后大端 width 字节；
 * unsigned 原样大端。编码字节按无符号字典序 = 数值序，可直接比字节。8 字节 unsigned 以原始 long bits 承载。
 */
public final class IntegerCodec implements TypeCodec {

    private final int width;
    private final boolean unsigned;

    public IntegerCodec(int width, boolean unsigned) {
        if (width != 1 && width != 2 && width != 4 && width != 8) {
            throw new DatabaseValidationException("integer width must be 1/2/4/8: " + width);
        }
        this.width = width;
        this.unsigned = unsigned;
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
        if (!(value instanceof ColumnValue.IntValue iv)) {
            throw new InvalidColumnValueException("expected IntValue for " + type.typeId());
        }
        long v = iv.value();
        if (!unsigned) {
            if (width < 8) {
                long min = -(1L << (width * 8 - 1));
                long max = (1L << (width * 8 - 1)) - 1;
                if (v < min || v > max) {
                    throw new ColumnValueOutOfRangeException("signed " + width + "B out of range: " + v);
                }
            }
        } else {
            if (width < 8) {
                long max = (1L << (width * 8)) - 1;
                if (v < 0 || v > max) {
                    throw new ColumnValueOutOfRangeException("unsigned " + width + "B out of range: " + v);
                }
            }
        }
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        validate(value, type);
        long v = ((ColumnValue.IntValue) value).value();
        long stored = unsigned ? v : (v ^ signBit());
        for (int i = width - 1; i >= 0; i--) {
            writer.putByte((int) ((stored >>> (i * 8)) & 0xFF));
        }
    }

    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        long stored = 0;
        for (int i = 0; i < width; i++) {
            stored = (stored << 8) | slice.byteAt(i);
        }
        long v;
        if (unsigned) {
            v = stored;
        } else {
            long x = stored ^ signBit();
            int shift = 64 - width * 8;
            v = (shift == 0) ? x : (x << shift) >> shift;
        }
        return new ColumnValue.IntValue(v);
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return FieldSlice.compareUnsigned(left, right);
    }

    private long signBit() {
        return 1L << (width * 8 - 1);
    }
}
