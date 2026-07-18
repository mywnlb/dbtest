package cn.zhangyis.db.storage.record.type;

/** 二进制排序规则：无符号字节字典序。单例。 */
public final class BinaryCollation implements CollationStrategy {

    /** 共享单例（无状态）。 */
    public static final BinaryCollation INSTANCE = new BinaryCollation();

    private BinaryCollation() {
    }

    /**
     * 实现 {@code compare} 的稳定值语义；比较只读取输入与本对象，不改变记录格式与页内组织状态。
     *
     * @param a 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param aOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param aLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param b 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param bOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param bLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     */
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
