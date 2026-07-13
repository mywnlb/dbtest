package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/** SET bitmap 定长 codec：最多 64 个 member，按最小 byteWidth unsigned big-endian 编码。 */
public final class SetCodec implements TypeCodec {

    /** schema dictionary member 数，也就是允许使用的低位数量。 */
    private final int memberCount;

    /** 页内固定 bitmap byte 数。 */
    private final int width;

    /** 创建绑定单一字典规模的 codec。 */
    public SetCodec(int memberCount) {
        if (memberCount < 1 || memberCount > 64) {
            throw new DatabaseValidationException("SET member count out of range: " + memberCount);
        }
        this.memberCount = memberCount;
        this.width = (memberCount + 7) / 8;
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
        if (!(value instanceof ColumnValue.SetValue setValue)) {
            throw new InvalidColumnValueException("expected SetValue for " + type.typeId());
        }
        if (type.typeId() != TypeId.SET || type.symbols().size() != memberCount) {
            throw new InvalidColumnValueException("SET codec/type dictionary mismatch");
        }
        if (memberCount < 64 && (setValue.bitmap() >>> memberCount) != 0) {
            throw new ColumnValueOutOfRangeException(
                    "SET bitmap uses bits above member count " + memberCount);
        }
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        validate(value, type);
        long bitmap = ((ColumnValue.SetValue) value).bitmap();
        for (int i = width - 1; i >= 0; i--) {
            writer.putByte((int) (bitmap >>> (i * 8)));
        }
    }

    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        long bitmap = 0;
        for (int i = 0; i < width; i++) {
            bitmap = (bitmap << 8) | slice.byteAt(i);
        }
        return new ColumnValue.SetValue(bitmap);
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return FieldSlice.compareUnsigned(left, right);
    }
}
