package cn.zhangyis.db.storage.record.type;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * UTF-8 教学 Unicode 主权重 V1。权重规则完全固化在源码中：ASCII/Latin-1、Greek、Cyrillic case fold，常用
 * Latin-1 accent 折叠，combining diacritical marks 忽略；其它 code point 以自身值为权重。禁止改写 V1 规则，
 * 未来扩展必须新增 stable collation id，避免已建索引顺序漂移。
 *
 * <p>它不是 MySQL UCA collation：不处理 locale tailoring、contraction、expansion 或多级权重，但对任意合法
 * Unicode 字符都有确定 fallback 次序。输入必须是严格 UTF-8，损坏字节不会 replacement 后继续比较。
 */
public final class UnicodeWeightCollationV1 implements CollationStrategy {

    /** 无状态、线程安全共享实例。 */
    public static final UnicodeWeightCollationV1 INSTANCE = new UnicodeWeightCollationV1();

    private UnicodeWeightCollationV1() {
    }

    /** 严格解码两侧 UTF-8、生成固定主权重序列，再做整数 lexicographic compare。 */
    @Override
    public int compare(byte[] a, int aOffset, int aLength, byte[] b, int bOffset, int bLength) {
        int[] left = weights(decode(a, aOffset, aLength));
        int[] right = weights(decode(b, bOffset, bLength));
        int common = Math.min(left.length, right.length);
        for (int i = 0; i < common; i++) {
            int compared = Integer.compare(left[i], right[i]);
            if (compared != 0) {
                return compared;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    /**
     * 把固定 V1 主权重序列编码为 big-endian int 数组。compare 相等当且仅当该序列逐项相等，
     * 因而 accent/case/combining-mark 等价值会竞争同一 unique-key 事务锁。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>用 REPORT 模式严格解码指定 UTF-8 slice，并按版本固定规则生成主权重序列。</li>
     *     <li>把每个权重编码为 big-endian 四字节，保持权重边界和 compare 相等关系。</li>
     * </ol>
     *
     * @param bytes  包含 UTF-8 字段 slice 的底层字节。
     * @param offset 字段 slice 起始偏移。
     * @param length 字段 slice 字节长度。
     * @return 每个 V1 主权重占四字节的新数组；compare==0 的输入得到相同数组。
     * @throws DatabaseValidationException slice 不是合法 UTF-8 时由严格 decoder 包装抛出。
     */
    @Override
    public byte[] equalityKey(byte[] bytes, int offset, int length) {
        // 1. 严格 UTF-8 解码与固定 V1 权重生成必须复用 compare 路径，避免锁判等分叉。
        int[] weights = weights(decode(bytes, offset, length));

        // 2. 定长 big-endian framing 防止不同权重序列产生字节边界歧义。
        byte[] key = new byte[weights.length * Integer.BYTES];
        int cursor = 0;
        for (int value : weights) {
            key[cursor++] = (byte) (value >>> 24);
            key[cursor++] = (byte) (value >>> 16);
            key[cursor++] = (byte) (value >>> 8);
            key[cursor++] = (byte) value;
        }
        return key;
    }

    /** 每次创建独立 decoder，避免共享 CharsetDecoder 的可变状态进入并发索引比较。 */
    private static String decode(byte[] bytes, int offset, int length) {
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes, offset, length));
            return decoded.toString();
        } catch (CharacterCodingException error) {
            throw new InvalidCharacterEncodingException(
                    "collation UTF8_UNICODE_CI_V1 received malformed UTF-8", error);
        }
    }

    /** 把 code point 转成单一主权重；combining mark 返回 -1 表示不参与主权重序列。 */
    private static int weight(int codePoint) {
        if (codePoint >= 0x0300 && codePoint <= 0x036F) {
            return -1;
        }
        if (codePoint >= 'A' && codePoint <= 'Z') {
            return codePoint + ('a' - 'A');
        }
        if ((codePoint >= 0x0391 && codePoint <= 0x03A1)
                || (codePoint >= 0x03A3 && codePoint <= 0x03AB)) {
            return codePoint + 0x20;
        }
        if (codePoint == 0x03C2) {
            return 0x03C3;
        }
        if (codePoint >= 0x0410 && codePoint <= 0x042F) {
            return codePoint + 0x20;
        }
        if (codePoint == 0x0401) {
            return 0x0451;
        }
        return switch (codePoint) {
            case 0x00C0, 0x00C1, 0x00C2, 0x00C3, 0x00C4, 0x00C5,
                    0x00E0, 0x00E1, 0x00E2, 0x00E3, 0x00E4, 0x00E5 -> 'a';
            case 0x00C7, 0x00E7 -> 'c';
            case 0x00C8, 0x00C9, 0x00CA, 0x00CB,
                    0x00E8, 0x00E9, 0x00EA, 0x00EB -> 'e';
            case 0x00CC, 0x00CD, 0x00CE, 0x00CF,
                    0x00EC, 0x00ED, 0x00EE, 0x00EF -> 'i';
            case 0x00D1, 0x00F1 -> 'n';
            case 0x00D2, 0x00D3, 0x00D4, 0x00D5, 0x00D6, 0x00D8,
                    0x00F2, 0x00F3, 0x00F4, 0x00F5, 0x00F6, 0x00F8 -> 'o';
            case 0x00D9, 0x00DA, 0x00DB, 0x00DC,
                    0x00F9, 0x00FA, 0x00FB, 0x00FC -> 'u';
            case 0x00DD, 0x00FD, 0x00FF -> 'y';
            default -> codePoint;
        };
    }

    /** 生成无 combining mark 的紧凑权重数组；不使用 JDK Unicode normalization table。 */
    private static int[] weights(String value) {
        int[] codePoints = value.codePoints().toArray();
        int count = 0;
        for (int codePoint : codePoints) {
            if (weight(codePoint) >= 0) {
                count++;
            }
        }
        int[] weights = new int[count];
        int index = 0;
        for (int codePoint : codePoints) {
            int weight = weight(codePoint);
            if (weight >= 0) {
                weights[index++] = weight;
            }
        }
        return weights;
    }
}
