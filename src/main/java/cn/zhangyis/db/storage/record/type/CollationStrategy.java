package cn.zhangyis.db.storage.record.type;

/**
 * 排序规则策略（innodb-record-design §8.3）。字符/二进制比较必须经此，不直接用 String.compareTo。
 * 当前包含 {@link BinaryCollation} 与确定性的 {@link AsciiCaseInsensitiveCollation}；Unicode weight 留后续。
 */
public interface CollationStrategy {

    /** 比较两段字节，返回 &lt;0/0/&gt;0。 */
    int compare(byte[] a, int aOffset, int aLength, byte[] b, int bOffset, int bLength);
}
