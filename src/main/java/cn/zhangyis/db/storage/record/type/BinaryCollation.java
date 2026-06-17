package cn.zhangyis.db.storage.record.type;

/** 二进制排序规则：无符号字节字典序。单例。 */
public final class BinaryCollation implements CollationStrategy {

    /** 共享单例（无状态）。 */
    public static final BinaryCollation INSTANCE = new BinaryCollation();

    private BinaryCollation() {
    }

    @Override
    public int compare(byte[] a, int aOffset, int aLength, byte[] b, int bOffset, int bLength) {
        int n = Math.min(aLength, bLength);
        for (int i = 0; i < n; i++) {
            int d = (a[aOffset + i] & 0xFF) - (b[bOffset + i] & 0xFF);
            if (d != 0) {
                return d;
            }
        }
        return Integer.compare(aLength, bLength);
    }
}
