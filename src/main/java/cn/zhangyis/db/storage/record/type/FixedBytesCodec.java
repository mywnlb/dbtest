package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 定长字节 codec：CHAR(padByte=空格, asString=true) / BINARY(padByte=0x00, asString=false)。
 * 编码补齐到 n 字节；超长抛 {@link ColumnValueOutOfRangeException}。CHAR 解码去尾部空格（MySQL 语义），BINARY 保留全 n 字节。
 * 字符模式按 ColumnType 声明的 charset 严格编解码并经其 collation 比较；二进制模式保持原始无符号字节序。
 */
public final class FixedBytesCodec implements TypeCodec {

    private final int nBytes;
    private final byte padByte;
    private final boolean asString;

    /** 只读字符服务；仅字符模式使用，负责严格 charset 与精确 collation pair。 */
    private final CharacterTypeRegistry characters;

    public FixedBytesCodec(int nBytes, byte padByte, boolean asString) {
        this(nBytes, padByte, asString, CharacterTypeRegistry.defaults());
    }

    /**
     * 创建绑定字符服务的定长 codec；由 {@link TypeCodecRegistry} 注入其共享只读 registry。
     */
    FixedBytesCodec(int nBytes, byte padByte, boolean asString, CharacterTypeRegistry characters) {
        if (nBytes < 1) {
            throw new DatabaseValidationException("fixed bytes length must be positive: " + nBytes);
        }
        if (characters == null) {
            throw new DatabaseValidationException("character type registry must not be null");
        }
        this.nBytes = nBytes;
        this.padByte = padByte;
        this.asString = asString;
        this.characters = characters;
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
            return new ColumnValue.StringValue(
                    characters.decode(new FieldSlice(b, 0, end), type.charset()));
        }
        return new ColumnValue.BinaryValue(b);
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        CollationStrategy collation = asString
                ? characters.collationFor(type.charset(), type.collation())
                : BinaryCollation.INSTANCE;
        return collation.compare(left.backing(), left.offset(), left.length(),
                right.backing(), right.offset(), right.length());
    }

    private byte[] bytesOf(ColumnValue value, ColumnType type) {
        if (asString) {
            if (!(value instanceof ColumnValue.StringValue sv)) {
                throw new InvalidColumnValueException("expected StringValue for " + type.typeId());
            }
            return characters.encode(sv.value(), type.charset());
        }
        if (!(value instanceof ColumnValue.BinaryValue bv)) {
            throw new InvalidColumnValueException("expected BinaryValue for " + type.typeId());
        }
        return bv.value();
    }
}
