package cn.zhangyis.db.storage.record.type;

import java.util.Arrays;

/**
 * 排序规则策略（innodb-record-design §8.3）。字符/二进制比较必须经此，不直接用 String.compareTo。
 * 当前包含 binary、ASCII-CI 与版本化 {@link UnicodeWeightCollationV1}；实现必须确定且不可依赖默认 locale。
 */
public interface CollationStrategy {

    /** 比较两段字节，返回 &lt;0/0/&gt;0。 */
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
