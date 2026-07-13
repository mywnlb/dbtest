package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/** ENUM 1-based ordinal 定长 codec：N<=255 使用 1B，否则使用 2B unsigned big-endian。 */
public final class EnumCodec implements TypeCodec {

    /** schema dictionary member 数。 */
    private final int memberCount;

    /** ordinal 的页内固定宽度。 */
    private final int width;

    /** 创建绑定单一字典规模的 codec。 */
    public EnumCodec(int memberCount) {
        if (memberCount < 1 || memberCount > 65_535) {
            throw new DatabaseValidationException("ENUM member count out of range: " + memberCount);
        }
        this.memberCount = memberCount;
        this.width = memberCount <= 255 ? 1 : 2;
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
        if (!(value instanceof ColumnValue.EnumValue enumValue)) {
            throw new InvalidColumnValueException("expected EnumValue for " + type.typeId());
        }
        if (type.typeId() != TypeId.ENUM || type.symbols().size() != memberCount) {
            throw new InvalidColumnValueException("ENUM codec/type dictionary mismatch");
        }
        if (enumValue.ordinal() < 1 || enumValue.ordinal() > memberCount) {
            throw new ColumnValueOutOfRangeException(
                    "ENUM ordinal out of range 1.." + memberCount + ": " + enumValue.ordinal());
        }
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        validate(value, type);
        int ordinal = ((ColumnValue.EnumValue) value).ordinal();
        for (int i = width - 1; i >= 0; i--) {
            writer.putByte(ordinal >>> (i * 8));
        }
    }

    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        int ordinal = 0;
        for (int i = 0; i < width; i++) {
            ordinal = (ordinal << 8) | slice.byteAt(i);
        }
        return new ColumnValue.EnumValue(ordinal);
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return FieldSlice.compareUnsigned(left, right);
    }
}
