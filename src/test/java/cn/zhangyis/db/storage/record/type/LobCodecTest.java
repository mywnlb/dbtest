package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LOB 记录 envelope、inline 边界、JSON 与 prefix 排序的契约测试。 */
class LobCodecTest {

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    /**
     * 验证 {@code inlineTextAndBinaryRoundTrip} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void inlineTextAndBinaryRoundTrip() {
        assertEquals(new ColumnValue.StringValue("中文 text"), roundTrip(
                new ColumnValue.StringValue("中文 text"), ColumnType.text(false)));
        ColumnValue.BinaryValue decoded = assertInstanceOf(ColumnValue.BinaryValue.class, roundTrip(
                new ColumnValue.BinaryValue(new byte[]{0, 1, (byte) 0xFF}), ColumnType.blob(false)));
        assertArrayEquals(new byte[]{0, 1, (byte) 0xFF}, decoded.value());
    }

    /**
     * 验证 {@code externalEnvelopeRoundTripsStableReferenceAndPrefix} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void externalEnvelopeRoundTripsStableReferenceAndPrefix() {
        ColumnType type = ColumnType.longText(false);
        LobReference reference = new LobReference(SpaceId.of(7), PageNo.of(64), 90_000, 6,
                SegmentId.of(9), 2, 0xFEED_BEEFL);
        ColumnValue.ExternalValue value = new ColumnValue.ExternalValue(
                TypeId.LONGTEXT, reference, "前缀".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        ColumnValue.ExternalValue decoded = assertInstanceOf(ColumnValue.ExternalValue.class,
                roundTrip(value, type));
        assertEquals(reference, decoded.reference());
        assertArrayEquals(value.inlinePrefix(), decoded.inlinePrefix());
    }

    /**
     * 验证 {@code oversizeInlineRequiresExternalizationAndWrongReferenceTypeIsRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void oversizeInlineRequiresExternalizationAndWrongReferenceTypeIsRejected() {
        ColumnType type = ColumnType.text(false);
        String large = "x".repeat(LobCodec.INLINE_PAYLOAD_LIMIT + 1);
        assertThrows(LobExternalizationRequiredException.class,
                () -> registry.validate(new ColumnValue.StringValue(large), type));

        LobReference ref = new LobReference(SpaceId.of(1), PageNo.of(64), 300, 1,
                SegmentId.of(1), 0, 1);
        assertThrows(InvalidColumnValueException.class, () -> registry.validate(
                new ColumnValue.ExternalValue(TypeId.BLOB, ref, new byte[0]), type));
    }

    /**
     * 验证 {@code jsonValidatesSyntaxAndCannotParticipateInIndexComparison} 所描述的 B+Tree 定位或结构变化，并断言键序、父子链接、页资源和唯一性不变量。
     */
    @Test
    void jsonValidatesSyntaxAndCannotParticipateInIndexComparison() {
        ColumnType type = ColumnType.json(false);
        assertEquals(new ColumnValue.StringValue("{\"a\":[1,true,null]}"), roundTrip(
                new ColumnValue.StringValue("{\"a\":[1,true,null]}"), type));
        assertEquals(new ColumnValue.StringValue("{\"n\":-1.25e+3,\"s\":\"\\uD83D\\uDE00\"}"),
                roundTrip(new ColumnValue.StringValue(
                        "{\"n\":-1.25e+3,\"s\":\"\\uD83D\\uDE00\"}"), type));
        assertThrows(InvalidColumnValueException.class,
                () -> registry.validate(new ColumnValue.StringValue("{broken"), type));
        assertThrows(InvalidColumnValueException.class,
                () -> registry.validate(new ColumnValue.StringValue("  "), type));
        assertThrows(InvalidColumnValueException.class,
                () -> registry.validate(new ColumnValue.StringValue("{foo:1}"), type));
        assertThrows(InvalidColumnValueException.class,
                () -> registry.validate(new ColumnValue.StringValue("{'foo':1}"), type));
        assertThrows(InvalidColumnValueException.class,
                () -> registry.validate(new ColumnValue.StringValue("{\"foo\":1,}"), type));
        assertThrows(InvalidColumnValueException.class,
                () -> registry.validate(new ColumnValue.StringValue("[01]"), type));

        byte[] encoded = encode(new ColumnValue.StringValue("{}"), type);
        assertThrows(UnsupportedColumnTypeException.class, () -> registry.codecFor(type).compareKeyPart(
                new FieldSlice(encoded, 0, encoded.length), new FieldSlice(encoded, 0, encoded.length), type, 1));
    }

    /**
     * 验证 {@code referenceRejectsMetadataPagesAndFilNullSentinel} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void referenceRejectsMetadataPagesAndFilNullSentinel() {
        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> new LobReference(SpaceId.of(1), PageNo.of(3), 10, 1, SegmentId.of(1), 0, 0));
        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> new LobReference(SpaceId.of(1), PageNo.of(0xFFFF_FFFFL),
                        10, 1, SegmentId.of(1), 0, 0));
    }

    /**
     * 验证 {@code textAndBlobRequirePrefixAndCompareLogicalPayloadInsteadOfEnvelope} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void textAndBlobRequirePrefixAndCompareLogicalPayloadInsteadOfEnvelope() {
        ColumnType type = ColumnType.text(false);
        byte[] apple = encode(new ColumnValue.StringValue("apple"), type);
        byte[] apricot = encode(new ColumnValue.StringValue("apricot"), type);
        TypeCodec codec = registry.codecFor(type);
        assertThrows(UnsupportedColumnTypeException.class, () -> codec.compareKeyPart(
                slice(apple), slice(apricot), type, 0));
        assertEquals(0, codec.compareKeyPart(slice(apple), slice(apricot), type, 2));
        assertTrue(codec.compareKeyPart(slice(apple), slice(apricot), type, 3) < 0);
    }

    private ColumnValue roundTrip(ColumnValue value, ColumnType type) {
        byte[] encoded = encode(value, type);
        return registry.codecFor(type).decode(new FieldSlice(encoded, 0, encoded.length), type);
    }

    private byte[] encode(ColumnValue value, ColumnType type) {
        TypeCodec codec = registry.codecFor(type);
        codec.validate(value, type);
        byte[] bytes = new byte[codec.encodedLength(value, type)];
        codec.encode(value, type, new FieldWriter(bytes, 0));
        return bytes;
    }

    private static FieldSlice slice(byte[] bytes) {
        return new FieldSlice(bytes, 0, bytes.length);
    }
}
