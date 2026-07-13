package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/**
 * 时间 codec：DATE=4B signed epochDay，TIME/DATETIME/TIMESTAMP=8B signed long，YEAR=2B unsigned。
 * signed 值翻转最高符号位后大端，YEAR 原值大端，因此所有类型的 unsigned byte 次序都等于其时间线性序。
 *
 * <p><b>教学简化</b>：TIME 的 MySQL 业务范围、TIMESTAMP 的 session 时区转换和 YEAR 的 MySQL 合法显示范围
 * 均属于 SQL/session 层；Record 只维护定长物理范围与保序不变量。
 */
public final class TemporalCodec implements TypeCodec {

    /** 当前 codec 唯一接受并在 decode 时恢复的逻辑时间种类。 */
    private final TemporalKind kind;

    /** 字段固定物理宽度；RecordFieldResolver 依赖它定位后续定长列。 */
    private final int width;

    /** YEAR 为 unsigned，其他时间归一值均按 signed 编码。 */
    private final boolean unsigned;

    /**
     * 为单一时间种类创建无状态 codec；宽度和 signedness 在构造时冻结，实例可安全共享。
     */
    public TemporalCodec(TemporalKind kind) {
        if (kind == null) {
            throw new DatabaseValidationException("temporal kind must not be null");
        }
        this.kind = kind;
        this.width = switch (kind) {
            case YEAR -> 2;
            case DATE -> 4;
            case TIME, DATETIME, TIMESTAMP -> 8;
        };
        this.unsigned = kind == TemporalKind.YEAR;
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
        TypeId expectedType = switch (kind) {
            case DATE -> TypeId.DATE;
            case TIME -> TypeId.TIME;
            case DATETIME -> TypeId.DATETIME;
            case TIMESTAMP -> TypeId.TIMESTAMP;
            case YEAR -> TypeId.YEAR;
        };
        if (type.typeId() != expectedType) {
            throw new InvalidColumnValueException(
                    "temporal column type mismatch: expected " + expectedType + " got " + type.typeId());
        }
        if (tv.kind() != kind) {
            throw new InvalidColumnValueException("temporal kind mismatch: expected " + kind + " got " + tv.kind());
        }
        long n = tv.normalized();
        if (kind == TemporalKind.DATE && (n < Integer.MIN_VALUE || n > Integer.MAX_VALUE)) {
            throw new ColumnValueOutOfRangeException("DATE epochDay out of int range: " + n);
        }
        if (kind == TemporalKind.YEAR && (n < 0 || n > 0xFFFFL)) {
            throw new ColumnValueOutOfRangeException("YEAR unsigned 2B out of range: " + n);
        }
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        validate(value, type);
        long n = ((ColumnValue.TemporalValue) value).normalized();
        long stored = unsigned ? n : n ^ signBit();
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
        long n;
        if (unsigned) {
            n = stored;
        } else {
            long x = stored ^ signBit();
            int shift = 64 - width * 8;
            n = (shift == 0) ? x : (x << shift) >> shift;
        }
        return new ColumnValue.TemporalValue(kind, n);
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return FieldSlice.compareUnsigned(left, right);
    }

    /** 返回当前 signed 宽度的最高位掩码，供保序翻转与恢复共用。 */
    private long signBit() {
        return 1L << (width * 8 - 1);
    }
}
