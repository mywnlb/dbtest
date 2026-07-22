package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/**
 * DDL marker 保存的 schema 内容证明。对象拥有摘要数组，读取时也返回副本；因此 repository 快照、map key 和
 * recovery 比较不会被外部数组修改破坏。
 */
public final class DdlSchemaDigest {

    /** 解释 bytes 的持久算法，不允许由长度反向猜测。 */
    private final DdlDigestAlgorithm algorithm;
    /** 生成摘要前使用的 canonical schema image 版本。 */
    private final DdlSchemaCanonicalFormat canonicalFormat;
    /** 精确算法输出；只在构造时复制，此后不原地修改。 */
    private final byte[] bytes;

    /**
     * 构造一个完整摘要值。
     *
     * @param algorithm marker 显式保存的算法，必须非空
     * @param canonicalFormat canonical schema image 版本，必须非空
     * @param bytes 算法输出；长度必须与 algorithm 完全一致，构造器会复制
     * @throws DatabaseValidationException 字段为空或摘要长度错误时抛出
     */
    public DdlSchemaDigest(DdlDigestAlgorithm algorithm,
                           DdlSchemaCanonicalFormat canonicalFormat,
                           byte[] bytes) {
        if (algorithm == null || canonicalFormat == null || bytes == null
                || bytes.length != (algorithm == null ? 0 : algorithm.digestBytes())) {
            throw new DatabaseValidationException("invalid DDL schema digest fields/length");
        }
        this.algorithm = algorithm;
        this.canonicalFormat = canonicalFormat;
        this.bytes = bytes.clone();
    }

    /** @return 解释摘要 bytes 的稳定算法。 */
    public DdlDigestAlgorithm algorithm() {
        return algorithm;
    }

    /** @return 生成摘要时使用的 canonical schema image 版本。 */
    public DdlSchemaCanonicalFormat canonicalFormat() {
        return canonicalFormat;
    }

    /** @return 摘要字节的防御性副本，调用方可以自由修改该副本。 */
    public byte[] bytes() {
        return bytes.clone();
    }

    /**
     * 使用常量时间字节比较，避免恢复控制面因首个差异位置暴露不必要的时间特征。
     *
     * @param other 待比较对象；允许为空或其它类型
     * @return 算法、canonical format 与全部摘要字节均相同时为 true
     */
    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof DdlSchemaDigest that
                && algorithm == that.algorithm
                && canonicalFormat == that.canonicalFormat
                && MessageDigest.isEqual(bytes, that.bytes);
    }

    /** @return 与 equals 一致的稳定进程内 hash；该值不进入任何持久协议。 */
    @Override
    public int hashCode() {
        return Objects.hash(algorithm, canonicalFormat, Arrays.hashCode(bytes));
    }

    /** @return 不暴露摘要内容的诊断字符串，仅包含算法与 canonical format。 */
    @Override
    public String toString() {
        return "DdlSchemaDigest[algorithm=" + algorithm + ", canonicalFormat=" + canonicalFormat + "]";
    }
}
