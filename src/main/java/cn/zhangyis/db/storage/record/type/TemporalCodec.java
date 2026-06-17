package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 时间 codec：DATE=4B（归一 epochDay）/ DATETIME=8B（归一 epochMilli）。**保序编码**：归一 long 翻最高位符号位后大端，
 * 字节序 = 时间线性序（含 epoch 前负值）。
 */
public final class TemporalCodec implements TypeCodec {

    private final TemporalKind kind;
    private final int width;

    public TemporalCodec(TemporalKind kind) {
        this.kind = kind;
        this.width = kind == TemporalKind.DATE ? 4 : 8;
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
        if (!(value instanceof ColumnValue.TemporalValue tv)) {
            throw new InvalidColumnValueException("expected TemporalValue for " + type.typeId());
        }
        if (tv.kind() != kind) {
            throw new InvalidColumnValueException("temporal kind mismatch: expected " + kind + " got " + tv.kind());
        }
        if (width == 4) {
            long n = tv.normalized();
            if (n < Integer.MIN_VALUE || n > Integer.MAX_VALUE) {
                throw new ColumnValueOutOfRangeException("DATE epochDay out of int range: " + n);
            }
        }
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        validate(value, type);
        long n = ((ColumnValue.TemporalValue) value).normalized();
        long stored = n ^ (1L << (width * 8 - 1));
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
        long x = stored ^ (1L << (width * 8 - 1));
        int shift = 64 - width * 8;
        long n = (shift == 0) ? x : (x << shift) >> shift;
        return new ColumnValue.TemporalValue(kind, n);
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return FieldSlice.compareUnsigned(left, right);
    }
}
