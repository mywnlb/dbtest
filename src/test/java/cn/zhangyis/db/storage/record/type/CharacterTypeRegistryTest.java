package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.CharsetId;
import cn.zhangyis.db.storage.record.schema.CollationId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 只读字符服务测试：严格编解码、合法 pair 精确注册以及错配 collation 拒绝。 */
class CharacterTypeRegistryTest {

    private final CharacterTypeRegistry registry = CharacterTypeRegistry.defaults();

    @Test
    void encodesAndDecodesWithDeclaredCharset() {
        assertArrayEquals(new byte[] {(byte) 0xC3, (byte) 0xA9}, registry.encode("é", CharsetId.UTF8));
        assertArrayEquals(new byte[] {(byte) 0xE9}, registry.encode("é", CharsetId.LATIN1));
        byte[] latin1 = {(byte) 0xE9};
        assertEquals("é", registry.decode(new FieldSlice(latin1, 0, latin1.length), CharsetId.LATIN1));
    }

    @Test
    void rejectsUnmappableInputAndMalformedBytes() {
        assertThrows(InvalidCharacterEncodingException.class,
                () -> registry.encode("汉", CharsetId.LATIN1));
        byte[] incompleteUtf8 = {(byte) 0xC3};
        assertThrows(InvalidCharacterEncodingException.class,
                () -> registry.decode(new FieldSlice(incompleteUtf8, 0, 1), CharsetId.UTF8));
    }

    @Test
    void resolvesOnlyRegisteredCharsetCollationPairs() {
        assertSame(BinaryCollation.INSTANCE, registry.collationFor(CharsetId.UTF8, CollationId.BINARY));
        assertSame(BinaryCollation.INSTANCE, registry.collationFor(CharsetId.LATIN1, CollationId.BINARY));
        assertSame(AsciiCaseInsensitiveCollation.INSTANCE,
                registry.collationFor(CharsetId.UTF8, CollationId.UTF8_ASCII_CI));
        assertThrows(UnsupportedCollationException.class,
                () -> registry.collationFor(CharsetId.UTF8, CollationId.LATIN1_ASCII_CI));
    }
}
