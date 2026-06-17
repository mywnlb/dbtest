package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

import java.nio.charset.StandardCharsets;

/**
 * 定长字节 codec：CHAR(padByte=空格, asString=true) / BINARY(padByte=0x00, asString=false)。
 * 编码补齐到 n 字节；超长抛 {@link ColumnValueOutOfRangeException}。CHAR 解码去尾部空格（MySQL 语义），BINARY 保留全 n 字节。
 * 比较经 {@link BinaryCollation}（字节序）。
 */
public final class FixedBytesCodec implements TypeCodec {

    private final int nBytes;
    private final byte padByte;
    private final boolean asString;

    public FixedBytesCodec(int nBytes, byte padByte, boolean asString) {
        if (nBytes < 1) {
            throw new DatabaseValidationException("fixed bytes length must be positive: " + nBytes);
        }
        this.nBytes = nBytes;
        this.padByte = padByte;
        this.asString = asString;
    }

    @Override
    public int encodedLength(ColumnValue value, ColumnType type) {
        return nBytes;
    }

    @Override
    public int fixedWidth(ColumnType type) {
        return nBytes;
    }

    @Override
    public void validate(ColumnValue value, ColumnType type) {
        byte[] b = bytesOf(value, type);
        if (b.length > nBytes) {
            throw new ColumnValueOutOfRangeException("value too long for fixed(" + nBytes + "): " + b.length);
        }
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        byte[] b = bytesOf(value, type);
        if (b.length > nBytes) {
            throw new ColumnValueOutOfRangeException("value too long for fixed(" + nBytes + "): " + b.length);
        }
        writer.putBytes(b);
        for (int i = b.length; i < nBytes; i++) {
            writer.putByte(padByte & 0xFF);
        }
    }

    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        byte[] b = slice.copyBytes();
        if (asString) {
            int end = b.length;
            while (end > 0 && b[end - 1] == 0x20) {
                end--;
            }
            return new ColumnValue.StringValue(new String(b, 0, end, StandardCharsets.UTF_8));
        }
        return new ColumnValue.BinaryValue(b);
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
