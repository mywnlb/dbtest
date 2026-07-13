package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/**
 * `BIT(n)` 定长编码策略。位串从首字节最高位开始左对齐，最后字节未使用低位必须为 0；这一 canonical 约束确保
 * 同一逻辑位串只有一种页内字节表示。固定宽度下 unsigned byte lexicographic 即 bit string 自然序。
 */
public final class BitCodec implements TypeCodec {

    /** schema 声明的有效 bit 数。 */
    private final int bitWidth;

    /** 页内固定 byte 数，等于 ceil(bitWidth/8)。 */
    private final int byteWidth;

    /** 最后字节中必须保持为 0 的低位掩码；byte-aligned 时为 0。 */
    private final int unusedLowBitMask;

    /** 为 1..64 位的单一 schema 宽度创建无状态 codec。 */
    public BitCodec(int bitWidth) {
        if (bitWidth < 1 || bitWidth > 64) {
            throw new DatabaseValidationException("BIT width out of range 1..64: " + bitWidth);
        }
        this.bitWidth = bitWidth;
        this.byteWidth = (bitWidth + 7) / 8;
        int unusedLowBits = byteWidth * 8 - bitWidth;
        this.unusedLowBitMask = unusedLowBits == 0 ? 0 : (1 << unusedLowBits) - 1;
    }

    @Override
    public int encodedLength(ColumnValue value, ColumnType type) {
        return byteWidth;
    }

    @Override
    public int fixedWidth(ColumnType type) {
        return byteWidth;
    }

    /**
     * 校验逻辑值、schema bitWidth、精确 byteWidth 和 canonical 尾位；失败时不写任何目标字节。
     */
    @Override
    public void validate(ColumnValue value, ColumnType type) {
        if (!(value instanceof ColumnValue.BitValue bitValue)) {
            throw new InvalidColumnValueException("expected BitValue for " + type.typeId());
        }
        if (type.typeId() != TypeId.BIT || type.length() != bitWidth) {
            throw new InvalidColumnValueException(
                    "BIT codec/type width mismatch: codec=" + bitWidth + " type=" + type.typeId() + "(" + type.length() + ")");
        }
        byte[] bytes = bitValue.value();
        if (bytes.length != byteWidth) {
            throw new InvalidColumnValueException(
                    "BIT(" + bitWidth + ") requires " + byteWidth + " bytes, got " + bytes.length);
        }
        if ((bytes[byteWidth - 1] & unusedLowBitMask) != 0) {
            throw new InvalidColumnValueException(
                    "BIT(" + bitWidth + ") unused low bits must be zero");
        }
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        validate(value, type);
        writer.putBytes(((ColumnValue.BitValue) value).value());
    }

    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        return new ColumnValue.BitValue(slice.copyBytes());
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return FieldSlice.compareUnsigned(left, right);
    }
}
