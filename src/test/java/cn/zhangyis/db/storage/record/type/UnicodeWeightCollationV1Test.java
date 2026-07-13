package cn.zhangyis.db.storage.record.type;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** UTF8_UNICODE_CI_V1 的固定 case/accent weights、fallback 码点序和严格 UTF-8 损坏边界。 */
class UnicodeWeightCollationV1Test {

    private final UnicodeWeightCollationV1 collation = UnicodeWeightCollationV1.INSTANCE;

    @Test
    void foldsCaseAccentAndCombiningMarks() {
        assertEquals(0, compare("École", "e\u0301COLE"));
        assertEquals(0, compare("ÀÑÖ", "ano"));
        assertEquals(0, compare("ΑΒΓ", "αβγ"));
        assertEquals(0, compare("МОСКВА", "москва"));
    }

    @Test
    void unmappedCodePointsKeepStableCodePointOrder() {
        assertTrue(compare("中", "文") < 0);
        assertTrue(compare("😀", "😁") < 0);
    }

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
