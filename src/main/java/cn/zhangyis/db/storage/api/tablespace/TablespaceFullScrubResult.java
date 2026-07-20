package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.SerializedDictionaryInfo;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

/**
 * 已完成全部物理页校验且扫描前后文件属性稳定的候选结果。
 *
 * @param path 已扫描的规范绝对路径
 * @param fileSize 文件总字节数
 * @param lastModifiedMillis 扫描后属性快照的修改时间
 * @param fileKey 文件系统提供的稳定对象 identity 文本；不支持时为空串
 * @param pageCount 通过校验的物理页总数
 * @param spaceId page0 与所有已初始化页共同声明的 space identity
 * @param pageSize page0、文件长度与请求共同声明的页大小
 * @param spaceVersion page0 的表空间格式/生命周期版本
 * @param sdi page3 中通过 payload CRC 的 opaque 字典信息
 * @param fileDigest 整个文件按物理页顺序计算的 SHA-256 fingerprint
 */
public record TablespaceFullScrubResult(
        Path path,
        long fileSize,
        long lastModifiedMillis,
        String fileKey,
        int pageCount,
        SpaceId spaceId,
        PageSize pageSize,
        long spaceVersion,
        SerializedDictionaryInfo sdi,
        byte[] fileDigest) {

    /** SHA-256 固定长度。 */
    private static final int SHA256_BYTES = 32;

    public TablespaceFullScrubResult {
        if (path == null || fileSize <= 0 || lastModifiedMillis < 0 || fileKey == null || pageCount < 4
                || spaceId == null || pageSize == null || sdi == null
                || fileDigest == null || fileDigest.length != SHA256_BYTES) {
            throw new DatabaseValidationException("tablespace full scrub result is invalid");
        }
        path = path.toAbsolutePath().normalize();
        fileDigest = Arrays.copyOf(fileDigest, fileDigest.length);
    }

    /**
     * 返回文件摘要副本，避免 complete-scan token 建立后被外部修改。
     *
     * @return 长度恒为 32 的全文件 SHA-256 副本
     */
    @Override
    public byte[] fileDigest() {
        return Arrays.copyOf(fileDigest, fileDigest.length);
    }

    /**
     * 按全文件摘要内容比较 scrub 结果。
     *
     * @param other 待比较对象
     * @return 文件属性、物理身份、SDI 与摘要字节全部相同时为 {@code true}
     */
    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof TablespaceFullScrubResult that
                && fileSize == that.fileSize
                && lastModifiedMillis == that.lastModifiedMillis
                && pageCount == that.pageCount
                && spaceVersion == that.spaceVersion
                && path.equals(that.path)
                && fileKey.equals(that.fileKey)
                && spaceId.equals(that.spaceId)
                && pageSize.equals(that.pageSize)
                && sdi.equals(that.sdi)
                && Arrays.equals(fileDigest, that.fileDigest);
    }

    /**
     * 生成与内容相等语义一致的哈希值。
     *
     * @return 文件与物理领域字段及摘要内容的组合哈希
     */
    @Override
    public int hashCode() {
        return 31 * Objects.hash(path, fileSize, lastModifiedMillis, fileKey, pageCount,
                spaceId, pageSize, spaceVersion, sdi) + Arrays.hashCode(fileDigest);
    }
}
