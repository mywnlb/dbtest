package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 变长字节 codec：VARCHAR(asString=true) / VARBINARY(asString=false)。字段内只存字节本身，长度由 record.format 变长目录记录。
 * 超过 maxBytes 抛 {@link ColumnValueOutOfRangeException}。字符模式按 schema charset/collation，二进制模式按原始字节。
 */
public final class VarBytesCodec implements TypeCodec {

    private final int maxBytes;
    private final boolean asString;

    /** 只读字符服务；字符模式的严格编解码与排序策略均由它提供。 */
    private final CharacterTypeRegistry characters;

    public VarBytesCodec(int maxBytes, boolean asString) {
        this(maxBytes, asString, CharacterTypeRegistry.defaults());
    }

    /** 创建绑定共享字符服务的变长 codec；由类型 registry 使用。 */
    VarBytesCodec(int maxBytes, boolean asString, CharacterTypeRegistry characters) {
        if (maxBytes < 1) {
            throw new DatabaseValidationException("var bytes max length must be positive: " + maxBytes);
        }
        if (characters == null) {
            throw new DatabaseValidationException("character type registry must not be null");
        }
        this.maxBytes = maxBytes;
        this.asString = asString;
        this.characters = characters;
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
        return asString ? new ColumnValue.StringValue(characters.decode(new FieldSlice(b, 0, b.length), type.charset()))
                : new ColumnValue.BinaryValue(b);
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
