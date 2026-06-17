package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * NULL 位图（innodb-record-design §5.2）。位数 = schema 中 nullable 列数，按 nullable 列在 schema 中的顺序编号；1=NULL。
 */
public final class NullBitmap {

    private final int count;
    private final byte[] bits;

    /** 新建全 0（无 NULL）位图。 */
    public NullBitmap(int nullableCount) {
        if (nullableCount < 0) {
            throw new DatabaseValidationException("nullable count must be non-negative: " + nullableCount);
        }
        this.count = nullableCount;
        this.bits = new byte[byteLength(nullableCount)];
    }

    private NullBitmap(int count, byte[] bits) {
        this.count = count;
        this.bits = bits;
    }

    /** count 个 nullable 列所需字节数。 */
    public static int byteLength(int count) {
        return (count + 7) / 8;
    }

    /** 置第 i 个 nullable 列为 NULL。 */
    public void set(int i) {
        check(i);
        bits[i / 8] |= (byte) (1 << (i % 8));
    }

    /** 第 i 个 nullable 列是否 NULL。 */
    public boolean get(int i) {
        check(i);
        return (bits[i / 8] & (1 << (i % 8))) != 0;
    }

    /** 位图字节数。 */
    public int byteLength() {
        return bits.length;
    }

    /** 写位图到 buf 的 at 处。 */
    public void writeTo(byte[] buf, int at) {
        System.arraycopy(bits, 0, buf, at, bits.length);
    }

    /** 从 buf 的 at 处读 count 列的位图。 */
    public static NullBitmap readFrom(byte[] buf, int at, int count) {
        byte[] b = Arrays.copyOfRange(buf, at, at + byteLength(count));
        return new NullBitmap(count, b);
    }

    private void check(int i) {
        if (i < 0 || i >= count) {
            throw new DatabaseValidationException("null bitmap index out of range: " + i + " (count " + count + ")");
        }
    }
}
