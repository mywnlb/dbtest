package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.CharsetId;
import cn.zhangyis.db.storage.record.schema.CollationId;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 字符编码与 collation 的只读注册表。它是 Record 类型系统的权威字符语义入口：字符 codec 通过本类严格
 * 编解码，比较器通过精确 charset/collation pair 选策略；运行中没有注册或替换入口，避免同一索引排序漂移。
 */
public final class CharacterTypeRegistry {

    /** 默认完整注册表；内部 map 构造后不可变，所有 codec 可安全共享。 */
    private static final CharacterTypeRegistry DEFAULTS = new CharacterTypeRegistry(
            Map.of(CharsetId.UTF8, StandardCharsets.UTF_8, CharsetId.LATIN1, StandardCharsets.ISO_8859_1),
            Map.of(
                    new CharsetCollationKey(CharsetId.UTF8, CollationId.BINARY), BinaryCollation.INSTANCE,
                    new CharsetCollationKey(CharsetId.UTF8, CollationId.UTF8_ASCII_CI),
                    AsciiCaseInsensitiveCollation.INSTANCE,
                    new CharsetCollationKey(CharsetId.LATIN1, CollationId.BINARY), BinaryCollation.INSTANCE,
                    new CharsetCollationKey(CharsetId.LATIN1, CollationId.LATIN1_ASCII_CI),
                    AsciiCaseInsensitiveCollation.INSTANCE));

    /** charset id 到 Java 编码实现的不可变精确映射。 */
    private final Map<CharsetId, Charset> charsets;

    /** charset/collation pair 到比较策略的不可变精确映射。 */
    private final Map<CharsetCollationKey, CollationStrategy> collations;

    private CharacterTypeRegistry(Map<CharsetId, Charset> charsets,
                                  Map<CharsetCollationKey, CollationStrategy> collations) {
        this.charsets = Map.copyOf(charsets);
        this.collations = Map.copyOf(collations);
    }

    /**
     * 返回初始化后只读的默认字符服务。
     *
     * @return 可跨 codec/线程共享的不可变实例。
     */
    public static CharacterTypeRegistry defaults() {
        return DEFAULTS;
    }

    /**
     * 按声明 charset 严格编码字符串。编码器每次调用独立创建，因为 {@link java.nio.charset.CharsetEncoder} 非线程安全。
     *
     * @param value 逻辑字符串。
     * @param charsetId schema 声明的字符集。
     * @return 精确编码字节。
     * @throws InvalidCharacterEncodingException 输入包含 malformed surrogate 或 charset 不可映射字符时抛出。
     */
    public byte[] encode(String value, CharsetId charsetId) {
        if (value == null || charsetId == null) {
            throw new DatabaseValidationException("character value/charset must not be null");
        }
        Charset charset = charsetFor(charsetId);
        try {
            ByteBuffer encoded = charset.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException e) {
            throw new InvalidCharacterEncodingException(
                    "value cannot be encoded with charset " + charsetId, e);
        }
    }

    /**
     * 按声明 charset 严格解码记录字段；损坏字节不得替换后继续参与比较或返回上层。
     *
     * @param slice 记录字段字节。
     * @param charsetId schema 声明的字符集。
     * @return 解码字符串。
     * @throws InvalidCharacterEncodingException 字段不是该 charset 的合法字节序列时抛出。
     */
    public String decode(FieldSlice slice, CharsetId charsetId) {
        if (slice == null || charsetId == null) {
            throw new DatabaseValidationException("character slice/charset must not be null");
        }
        Charset charset = charsetFor(charsetId);
        try {
            return charset.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(slice.backing(), slice.offset(), slice.length()))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new InvalidCharacterEncodingException(
                    "record bytes cannot be decoded with charset " + charsetId, e);
        }
    }

    /**
     * 精确解析 charset/collation pair。缺失或错配时 fail-closed，禁止回退 BINARY。
     *
     * @param charsetId 字符集稳定标识。
     * @param collationId 排序规则稳定标识。
     * @return 已注册的无状态比较策略。
     * @throws UnsupportedCollationException pair 未注册时抛出。
     */
    public CollationStrategy collationFor(CharsetId charsetId, CollationId collationId) {
        if (charsetId == null || collationId == null) {
            throw new DatabaseValidationException("charset/collation must not be null");
        }
        CollationStrategy strategy = collations.get(new CharsetCollationKey(charsetId, collationId));
        if (strategy == null) {
            throw new UnsupportedCollationException(
                    "unsupported charset/collation pair: " + charsetId + "/" + collationId);
        }
        return strategy;
    }

    /** 查找 charset 实现；enum 有值但 registry 未装配时仍 fail-closed。 */
    private Charset charsetFor(CharsetId charsetId) {
        Charset charset = charsets.get(charsetId);
        if (charset == null) {
            throw new InvalidCharacterEncodingException("unsupported charset: " + charsetId);
        }
        return charset;
    }

    /** charset/collation 组合键；仅 registry 内使用，不暴露可变注册能力。 */
    private record CharsetCollationKey(CharsetId charsetId, CollationId collationId) {
    }
}
