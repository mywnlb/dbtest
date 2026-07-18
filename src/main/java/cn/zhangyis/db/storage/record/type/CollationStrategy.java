package cn.zhangyis.db.storage.record.type;

import java.util.Arrays;

/**
 * 排序规则策略（innodb-record-design §8.3）。字符/二进制比较必须经此，不直接用 String.compareTo。
 * 当前包含 binary、ASCII-CI 与版本化 {@link UnicodeWeightCollationV1}；实现必须确定且不可依赖默认 locale。
 */
public interface CollationStrategy {

    /** 比较两段字节，返回 &lt;0/0/&gt;0。
     *
     * @param a 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param aOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param aLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param b 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param bOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param bLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     */
    int compare(byte[] a, int aOffset, int aLength, byte[] b, int bOffset, int bLength);

    /**
     * 生成与 {@link #compare} 的“相等”关系一致的稳定字节键，供事务 unique-key 锁构造 identity。
     * 默认实现适用于 binary collation；大小写/权重折叠策略必须覆盖该方法，保证 compare==0 的输入得到相同键。
     *
     * @param bytes  包含待归一化字符/二进制字段的底层编码字节。
     * @param offset 字段 slice 在数组中的起始偏移。
     * @param length 字段 slice 的字节长度。
     * @return 与输入 slice 隔离的新字节数组；任意 compare==0 的两个 slice 必须返回逐字节相同的键。
     * @throws IndexOutOfBoundsException offset/length 越过数组边界时由数组复制操作抛出；调用方应先使用合法 FieldSlice。
     */
    default byte[] equalityKey(byte[] bytes, int offset, int length) {
        return Arrays.copyOfRange(bytes, offset, offset + length);
    }
}
