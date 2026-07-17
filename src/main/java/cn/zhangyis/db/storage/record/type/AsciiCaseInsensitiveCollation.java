package cn.zhangyis.db.storage.record.type;

/**
 * 确定性的教学型 case-insensitive collation：逐字节把 ASCII A-Z 折叠为 a-z，其余编码字节保持不变。
 *
 * <p>该策略刻意不使用 JDK {@code Collator}、locale 或 Unicode 数据表，避免 JDK/区域配置升级改变已建索引顺序。
 * 它不是 MySQL Unicode weight collation；优点是 byte-prefix 即使截在 UTF-8 多字节字符中间仍可确定比较。
 */
public final class AsciiCaseInsensitiveCollation implements CollationStrategy {

    /** 无状态共享单例。 */
    public static final AsciiCaseInsensitiveCollation INSTANCE = new AsciiCaseInsensitiveCollation();

    private AsciiCaseInsensitiveCollation() {
    }

    /** 按 ASCII fold 后的无符号字节字典序比较两个编码切片。 */
    @Override
    public int compare(byte[] a, int aOffset, int aLength, byte[] b, int bOffset, int bLength) {
        int n = Math.min(aLength, bLength);
        for (int i = 0; i < n; i++) {
            int d = fold(a[aOffset + i] & 0xFF) - fold(b[bOffset + i] & 0xFF);
            if (d != 0) {
                return d;
            }
        }
        return Integer.compare(aLength, bLength);
    }

    /**
     * 把 ASCII A-Z 折叠后的字节作为 equality key，使大小写等价值竞争同一个 unique-key 事务锁。
     *
     * @param bytes  包含 ASCII-CI 字段 slice 的底层字节。
     * @param offset 字段 slice 起始偏移。
     * @param length 字段 slice 字节长度。
     * @return 长度与输入相同的新数组；ASCII 大写转为小写，其它字节保持不变。
     * @throws IndexOutOfBoundsException offset/length 越过数组边界时抛出。
     */
    @Override
    public byte[] equalityKey(byte[] bytes, int offset, int length) {
        byte[] key = new byte[length];
        for (int i = 0; i < length; i++) {
            key[i] = (byte) fold(bytes[offset + i] & 0xFF);
        }
        return key;
    }

    /**
     * 折叠单个无符号字节的 ASCII 大写范围；高位字节、数字与标点保持不变。
     *
     * @param value 0..255 的无符号字节值。
     * @return A-Z 对应的小写值，其它输入原样返回。
     */
    private static int fold(int value) {
        return value >= 'A' && value <= 'Z' ? value + ('a' - 'A') : value;
    }
}
