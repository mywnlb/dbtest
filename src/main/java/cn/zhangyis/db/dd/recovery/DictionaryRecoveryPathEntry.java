package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

/**
 * clean manifest 中一张 ACTIVE 表的目录见证。
 *
 * @param tableId SDI 与 catalog 共同声明的正数表 identity
 * @param spaceId page0、storage binding 与文件名共同声明的正数表空间 identity
 * @param relativePath 相对 {@code tables/} 的规范路径；不得绝对化或包含父目录逃逸
 * @param sdiDigest 对确定性 DD SDI payload 计算的 SHA-256，用于拒绝身份相同但定义不同的候选
 */
public record DictionaryRecoveryPathEntry(long tableId, int spaceId, String relativePath, byte[] sdiDigest) {

    /** SHA-256 的固定字节数。 */
    private static final int SHA256_BYTES = 32;

    public DictionaryRecoveryPathEntry {
        if (tableId <= 0 || spaceId <= 0 || relativePath == null || relativePath.isBlank()
                || sdiDigest == null || sdiDigest.length != SHA256_BYTES) {
            throw new DatabaseValidationException("dictionary recovery path entry is invalid");
        }
        Path path = Path.of(relativePath).normalize();
        if (path.isAbsolute() || path.startsWith("..") || path.toString().isBlank()
                || path.getNameCount() != 1) {
            throw new DatabaseValidationException("dictionary recovery path escapes tables directory: " + relativePath);
        }
        relativePath = path.toString();
        sdiDigest = Arrays.copyOf(sdiDigest, sdiDigest.length);
    }

    /**
     * 返回独立摘要副本，防止 token/manifest 校验完成后被调用方修改。
     *
     * @return 长度恒为 32 的 SHA-256 副本
     */
    @Override
    public byte[] sdiDigest() {
        return Arrays.copyOf(sdiDigest, sdiDigest.length);
    }

    /**
     * 按摘要内容而不是数组引用比较路径见证。
     *
     * @param other 待比较对象
     * @return table/space/path 与 SDI 摘要字节都相同时为 {@code true}
     */
    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof DictionaryRecoveryPathEntry that
                && tableId == that.tableId
                && spaceId == that.spaceId
                && relativePath.equals(that.relativePath)
                && Arrays.equals(sdiDigest, that.sdiDigest);
    }

    /**
     * 生成与内容相等语义一致的哈希值。
     *
     * @return 全部 identity/path 与摘要内容的组合哈希
     */
    @Override
    public int hashCode() {
        return 31 * Objects.hash(tableId, spaceId, relativePath) + Arrays.hashCode(sdiDigest);
    }
}
