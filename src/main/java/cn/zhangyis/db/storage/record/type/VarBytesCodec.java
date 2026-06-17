package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

import java.nio.charset.StandardCharsets;

/**
 * 变长字节 codec：VARCHAR(asString=true) / VARBINARY(asString=false)。字段内只存字节本身，长度由 record.format 变长目录记录。
 * 超过 maxBytes 抛 {@link ColumnValueOutOfRangeException}。比较经 {@link BinaryCollation}（字节序）。
 */
public final class VarBytesCodec implements TypeCodec {

    private final int maxBytes;
    private final boolean asString;

    public VarBytesCodec(int maxBytes, boolean asString) {
        if (maxBytes < 1) {
            throw new DatabaseValidationException("var bytes max length must be positive: " + maxBytes);
        }
        this.maxBytes = maxBytes;
        this.asString = asString;
    }

    @Override
    public int encodedLength(ColumnValue value, ColumnType type) {
        return bytesOf(value, type).length;
    }

    @Override
    public int fixedWidth(ColumnType type) {
        throw new DatabaseValidationException("variable type has no fixed width: " + type.typeId());
    }

    @Override
    public void validate(ColumnValue value, ColumnType type) {
        byte[] b = bytesOf(value, type);
        if (b.length > maxBytes) {
            throw new ColumnValueOutOfRangeException("value too long for var(" + maxBytes + "): " + b.length);
        }
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        byte[] b = bytesOf(value, type);
        if (b.length > maxBytes) {
            throw new ColumnValueOutOfRangeException("value too long for var(" + maxBytes + "): " + b.length);
        }
        writer.putBytes(b);
    }

    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        byte[] b = slice.copyBytes();
        return asString ? new ColumnValue.StringValue(new String(b, StandardCharsets.UTF_8))
                : new ColumnValue.BinaryValue(b);
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return BinaryCollation.INSTANCE.compare(left.backing(), left.offset(), left.length(),
                right.backing(), right.offset(), right.length());
    }

    private byte[] bytesOf(ColumnValue value, ColumnType type) {
        if (asString) {
            if (!(value instanceof ColumnValue.StringValue sv)) {
                throw new InvalidColumnValueException("expected StringValue for " + type.typeId());
            }
            return sv.value().getBytes(StandardCharsets.UTF_8);
        }
        if (!(value instanceof ColumnValue.BinaryValue bv)) {
            throw new InvalidColumnValueException("expected BinaryValue for " + type.typeId());
        }
        return bv.value();
    }
}
