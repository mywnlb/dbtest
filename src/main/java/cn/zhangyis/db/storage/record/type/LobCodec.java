package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.json.StrictJsonValidator;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/**
 * TEXT/BLOB/JSON 的记录字段 codec。字段首字节区分 inline payload 与 external reference；因此页内记录不需要
 * 理解 FSP 或 Buffer Pool。external envelope 保留短 prefix，但完整值只能由 storage.api.lob.LobStorage 读取。
 */
public final class LobCodec implements TypeCodec {

    /** 记录内允许直接保存的最大逻辑 payload。 */
    public static final int INLINE_PAYLOAD_LIMIT = 256;
    /** external envelope 最多保存的逻辑前缀字节。 */
    public static final int EXTERNAL_PREFIX_LIMIT = 32;

    private static final int INLINE_TAG = 0;
    private static final int EXTERNAL_TAG = 1;
    private static final int REFERENCE_BYTES = 32;
    private static final int EXTERNAL_HEADER_BYTES = 1 + REFERENCE_BYTES + 1;

    /** 该 codec 绑定的物理类型，阻止不同 LOB family 的 external reference 交叉解释。 */
    private final TypeId typeId;
    /** 字符类型为 true，BLOB family 为 false。 */
    private final boolean text;
    /** JSON 需要额外语法校验且永远不可进入核心索引比较。 */
    private final boolean json;
    /** 共享、只读的严格字符服务。 */
    private final CharacterTypeRegistry characters;

    LobCodec(TypeId typeId, boolean text, boolean json, CharacterTypeRegistry characters) {
        if (typeId == null || characters == null) {
            throw new DatabaseValidationException("LOB codec type/character registry must not be null");
        }
        this.typeId = typeId;
        this.text = text;
        this.json = json;
        this.characters = characters;
    }

    @Override
    public int encodedLength(ColumnValue value, ColumnType type) {
        requireBoundType(type);
        if (value instanceof ColumnValue.ExternalValue external) {
            validateExternal(external, type);
            return EXTERNAL_HEADER_BYTES + external.inlinePrefix().length;
        }
        byte[] bytes = logicalBytes(value, type);
        requireInline(bytes.length, type);
        return 1 + bytes.length;
    }

    @Override
    public int fixedWidth(ColumnType type) {
        throw new DatabaseValidationException("overflow-capable type has no fixed width: " + type.typeId());
    }

    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        requireBoundType(type);
        if (value instanceof ColumnValue.ExternalValue external) {
            validateExternal(external, type);
            writer.putByte(EXTERNAL_TAG);
            writeReference(writer, external.reference());
            byte[] prefix = external.inlinePrefix();
            writer.putByte(prefix.length);
            writer.putBytes(prefix);
            return;
        }
        byte[] bytes = logicalBytes(value, type);
        requireInline(bytes.length, type);
        writer.putByte(INLINE_TAG);
        writer.putBytes(bytes);
    }

    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        requireBoundType(type);
        if (slice.length() < 1) {
            throw new InvalidColumnValueException("empty LOB record envelope for " + typeId);
        }
        return switch (slice.byteAt(0)) {
            case INLINE_TAG -> logicalValue(subSlice(slice, 1, slice.length() - 1), type);
            case EXTERNAL_TAG -> decodeExternal(slice, type);
            default -> throw new InvalidColumnValueException(
                    "unknown LOB record envelope tag " + slice.byteAt(0) + " for " + typeId);
        };
    }

    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        throw new UnsupportedColumnTypeException(
                type.typeId() + " requires an explicit prefix index; full LOB/JSON comparison is unsupported");
    }

    /**
     * LOB key 只比较 envelope 中可得的逻辑 prefix。prefix 超过 external 保存量时拒绝，绝不在持索引页 latch 时
     * 追读 LOB 页链；JSON 无论 prefix 与否都不可进入核心索引。
     */
    @Override
    public int compareKeyPart(FieldSlice left, FieldSlice right, ColumnType type, int prefixBytes) {
        requireBoundType(type);
        if (json) {
            throw new UnsupportedColumnTypeException("JSON cannot participate in core index comparison");
        }
        if (prefixBytes <= 0) {
            return compare(left, right, type);
        }
        FieldSlice leftPayload = comparablePayload(left, type, prefixBytes);
        FieldSlice rightPayload = comparablePayload(right, type, prefixBytes);
        int leftLength = safePrefixLength(leftPayload, type, prefixBytes);
        int rightLength = safePrefixLength(rightPayload, type, prefixBytes);
        FieldSlice leftPrefix = subSlice(leftPayload, 0, leftLength);
        FieldSlice rightPrefix = subSlice(rightPayload, 0, rightLength);
        CollationStrategy strategy = text
                ? characters.collationFor(type.charset(), type.collation()) : BinaryCollation.INSTANCE;
        return strategy.compare(leftPrefix.backing(), leftPrefix.offset(), leftPrefix.length(),
                rightPrefix.backing(), rightPrefix.offset(), rightPrefix.length());
    }

    @Override
    public void validate(ColumnValue value, ColumnType type) {
        requireBoundType(type);
        if (value instanceof ColumnValue.ExternalValue external) {
            validateExternal(external, type);
            return;
        }
        byte[] bytes = logicalBytes(value, type);
        requireInline(bytes.length, type);
    }

    /**
     * 把待 externalize 的完整逻辑值转成精确 payload；只校验 schema 最大长度，不施加记录 inline 上限。
     */
    public byte[] logicalBytesForStorage(ColumnValue value, ColumnType type) {
        requireBoundType(type);
        if (value instanceof ColumnValue.ExternalValue) {
            throw new InvalidColumnValueException("cannot externalize an existing external LOB reference");
        }
        return logicalBytes(value, type);
    }

    /** 把从页链校验完成的完整 payload 恢复为字符串或二进制逻辑值。 */
    public ColumnValue logicalValueFromStorage(byte[] payload, ColumnType type) {
        if (payload == null) {
            throw new DatabaseValidationException("LOB payload must not be null");
        }
        requireBoundType(type);
        if (payload.length > type.length()) {
            throw new ColumnValueOutOfRangeException("LOB payload exceeds " + typeId + " max bytes: " + payload.length);
        }
        return logicalValue(new FieldSlice(payload, 0, payload.length), type);
    }

    /** 在最多 32B budget 内生成字符边界安全的 inline prefix。 */
    public byte[] inlinePrefix(byte[] payload, ColumnType type) {
        if (payload == null) {
            throw new DatabaseValidationException("LOB prefix payload must not be null");
        }
        int length = Math.min(payload.length, EXTERNAL_PREFIX_LIMIT);
        if (text) {
            characters.decode(new FieldSlice(payload, 0, payload.length), type.charset());
            while (length > 0) {
                try {
                    characters.decode(new FieldSlice(payload, 0, length), type.charset());
                    break;
                } catch (InvalidCharacterEncodingException incompleteCharacter) {
                    length--;
                }
            }
        }
        byte[] prefix = new byte[length];
        System.arraycopy(payload, 0, prefix, 0, length);
        return prefix;
    }

    private byte[] logicalBytes(ColumnValue value, ColumnType type) {
        byte[] bytes;
        if (text) {
            if (!(value instanceof ColumnValue.StringValue stringValue)) {
                throw new InvalidColumnValueException("expected StringValue for " + typeId);
            }
            if (json) {
                validateJson(stringValue.value());
            }
            bytes = characters.encode(stringValue.value(), type.charset());
        } else {
            if (!(value instanceof ColumnValue.BinaryValue binaryValue)) {
                throw new InvalidColumnValueException("expected BinaryValue for " + typeId);
            }
            bytes = binaryValue.value();
        }
        if (bytes.length > type.length()) {
            throw new ColumnValueOutOfRangeException(
                    "LOB payload exceeds " + typeId + " max bytes " + type.length() + ": " + bytes.length);
        }
        return bytes;
    }

    private ColumnValue logicalValue(FieldSlice payload, ColumnType type) {
        if (payload.length() > type.length()) {
            throw new ColumnValueOutOfRangeException("encoded LOB exceeds schema max bytes: " + payload.length());
        }
        if (!text) {
            return new ColumnValue.BinaryValue(payload.copyBytes());
        }
        String value = characters.decode(payload, type.charset());
        if (json) {
            validateJson(value);
        }
        return new ColumnValue.StringValue(value);
    }

    private void validateExternal(ColumnValue.ExternalValue external, ColumnType type) {
        if (external.typeId() != typeId) {
            throw new InvalidColumnValueException(
                    "external LOB type mismatch: expected=" + typeId + ", actual=" + external.typeId());
        }
        byte[] prefix = external.inlinePrefix();
        if (prefix.length > EXTERNAL_PREFIX_LIMIT || prefix.length > external.reference().totalLength()) {
            throw new InvalidColumnValueException("external LOB inline prefix length is invalid: " + prefix.length);
        }
        if (external.reference().totalLength() > type.length()) {
            throw new ColumnValueOutOfRangeException("external LOB length exceeds schema max: "
                    + external.reference().totalLength());
        }
        if (text && prefix.length > 0) {
            characters.decode(new FieldSlice(prefix, 0, prefix.length), type.charset());
        }
    }

    private ColumnValue.ExternalValue decodeExternal(FieldSlice slice, ColumnType type) {
        if (slice.length() < EXTERNAL_HEADER_BYTES) {
            throw new InvalidColumnValueException("truncated external LOB envelope: " + slice.length());
        }
        LobReference reference = readReference(slice, 1);
        int prefixLength = slice.byteAt(1 + REFERENCE_BYTES);
        if (prefixLength > EXTERNAL_PREFIX_LIMIT || slice.length() != EXTERNAL_HEADER_BYTES + prefixLength) {
            throw new InvalidColumnValueException("external LOB prefix/envelope length mismatch: " + prefixLength);
        }
        ColumnValue.ExternalValue value = new ColumnValue.ExternalValue(typeId, reference,
                subSlice(slice, EXTERNAL_HEADER_BYTES, prefixLength).copyBytes());
        validateExternal(value, type);
        return value;
    }

    private FieldSlice comparablePayload(FieldSlice envelope, ColumnType type, int prefixBytes) {
        ColumnValue decoded = decode(envelope, type);
        byte[] payload;
        if (decoded instanceof ColumnValue.ExternalValue external) {
            payload = external.inlinePrefix();
            if (prefixBytes > payload.length && external.reference().totalLength() > payload.length) {
                throw new UnsupportedColumnTypeException("LOB prefix " + prefixBytes
                        + " exceeds inline external prefix " + payload.length);
            }
        } else {
            payload = logicalBytes(decoded, type);
        }
        return new FieldSlice(payload, 0, payload.length);
    }

    private int safePrefixLength(FieldSlice payload, ColumnType type, int prefixBytes) {
        int length = Math.min(payload.length(), prefixBytes);
        if (!text) {
            return length;
        }
        characters.decode(payload, type.charset());
        while (length > 0) {
            try {
                characters.decode(subSlice(payload, 0, length), type.charset());
                return length;
            } catch (InvalidCharacterEncodingException incompleteCharacter) {
                length--;
            }
        }
        return 0;
    }

    private void requireInline(int length, ColumnType type) {
        if (length > INLINE_PAYLOAD_LIMIT) {
            throw new LobExternalizationRequiredException(
                    type.typeId() + " payload " + length + "B exceeds inline limit " + INLINE_PAYLOAD_LIMIT);
        }
    }

    private void requireBoundType(ColumnType type) {
        if (type == null || type.typeId() != typeId) {
            throw new DatabaseValidationException("LOB codec bound type mismatch: expected=" + typeId
                    + ", actual=" + (type == null ? null : type.typeId()));
        }
    }

    private static void validateJson(String value) {
        if (value == null || value.isBlank()) {
            throw new InvalidColumnValueException("JSON text must not be blank");
        }
        try {
            StrictJsonValidator.validate(value);
        } catch (RuntimeException parseFailure) {
            throw new InvalidColumnValueException("invalid JSON text", parseFailure);
        }
    }

    private static void writeReference(FieldWriter writer, LobReference reference) {
        putInt(writer, reference.spaceId().value());
        putInt(writer, checkedU32PageNo(reference.firstPageNo()));
        putInt(writer, reference.totalLength());
        putInt(writer, reference.pageCount());
        putLong(writer, reference.segmentId().value());
        putInt(writer, reference.inodeSlot());
        putInt(writer, (int) reference.crc32());
    }

    private static LobReference readReference(FieldSlice slice, int offset) {
        int spaceId = readInt(slice, offset);
        long pageNo = readInt(slice, offset + 4) & 0xFFFF_FFFFL;
        int totalLength = readInt(slice, offset + 8);
        int pageCount = readInt(slice, offset + 12);
        long segmentId = readLong(slice, offset + 16);
        int inodeSlot = readInt(slice, offset + 24);
        long crc32 = readInt(slice, offset + 28) & 0xFFFF_FFFFL;
        return new LobReference(SpaceId.of(spaceId), PageNo.of(pageNo), totalLength, pageCount,
                SegmentId.of(segmentId), inodeSlot, crc32);
    }

    private static int checkedU32PageNo(PageNo pageNo) {
        if (pageNo.value() > 0xFFFF_FFFFL) {
            throw new DatabaseValidationException("LOB first page exceeds FIL u32 range: " + pageNo.value());
        }
        return (int) pageNo.value();
    }

    private static void putInt(FieldWriter writer, int value) {
        writer.putByte(value >>> 24);
        writer.putByte(value >>> 16);
        writer.putByte(value >>> 8);
        writer.putByte(value);
    }

    private static void putLong(FieldWriter writer, long value) {
        putInt(writer, (int) (value >>> 32));
        putInt(writer, (int) value);
    }

    private static int readInt(FieldSlice slice, int offset) {
        return (slice.byteAt(offset) << 24) | (slice.byteAt(offset + 1) << 16)
                | (slice.byteAt(offset + 2) << 8) | slice.byteAt(offset + 3);
    }

    private static long readLong(FieldSlice slice, int offset) {
        return ((long) readInt(slice, offset) << 32) | (readInt(slice, offset + 4) & 0xFFFF_FFFFL);
    }

    private static FieldSlice subSlice(FieldSlice source, int relativeOffset, int length) {
        if (relativeOffset < 0 || length < 0 || relativeOffset + length > source.length()) {
            throw new InvalidColumnValueException("LOB envelope slice out of bounds");
        }
        return new FieldSlice(source.backing(), source.offset() + relativeOffset, length);
    }
}
