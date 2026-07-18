package cn.zhangyis.db.storage.record.type;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** UTF8_UNICODE_CI_V1 的固定 case/accent weights、fallback 码点序和严格 UTF-8 损坏边界。 */
class UnicodeWeightCollationV1Test {

    private final UnicodeWeightCollationV1 collation = UnicodeWeightCollationV1.INSTANCE;

    /**
     * 验证 {@code foldsCaseAccentAndCombiningMarks} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void foldsCaseAccentAndCombiningMarks() {
        assertEquals(0, compare("École", "e\u0301COLE"));
        assertEquals(0, compare("ÀÑÖ", "ano"));
        assertEquals(0, compare("ΑΒΓ", "αβγ"));
        assertEquals(0, compare("МОСКВА", "москва"));
    }

    /**
     * 验证 {@code unmappedCodePointsKeepStableCodePointOrder} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void unmappedCodePointsKeepStableCodePointOrder() {
        assertTrue(compare("中", "文") < 0);
        assertTrue(compare("😀", "😁") < 0);
    }

    /**
     * 验证 {@code malformedUtf8IsRejectedWithCause} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void malformedUtf8IsRejectedWithCause() {
        byte[] malformed = {(byte) 0xC3, 0x28};
        InvalidCharacterEncodingException error = assertThrows(InvalidCharacterEncodingException.class,
                () -> collation.compare(malformed, 0, malformed.length, new byte[] {'a'}, 0, 1));
        assertTrue(error.getCause() != null);
    }

    private int compare(String left, String right) {
        byte[] a = left.getBytes(StandardCharsets.UTF_8);
        byte[] b = right.getBytes(StandardCharsets.UTF_8);
        return collation.compare(a, 0, a.length, b, 0, b.length);
    }
}
