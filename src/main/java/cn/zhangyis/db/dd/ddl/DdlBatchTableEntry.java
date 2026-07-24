package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;

/**
 * 批量 DROP marker 中一张表的不可变恢复证据。
 *
 * @param tableId catalog 中不可复用的表 identity
 * @param spaceId page0 与受控文件名共同证明的物理空间 identity
 * @param relativePath 相对 {@code tables/} 的单层受控文件名；恢复时必须重新解析并校验
 * @param rowFormatVersion source/pending/target digest 共同使用的物理 record 格式版本
 * @param sourceSchemaDigest ACTIVE 聚合的内容摘要
 * @param pendingSchemaDigest DROP_PENDING 聚合的内容摘要
 * @param targetSchemaDigest DROPPED tombstone 聚合的内容摘要
 */
public record DdlBatchTableEntry(
        TableId tableId,
        SpaceId spaceId,
        String relativePath,
        long rowFormatVersion,
        DdlSchemaDigest sourceSchemaDigest,
        DdlSchemaDigest pendingSchemaDigest,
        DdlSchemaDigest targetSchemaDigest) {

    /**
     * 冻结单表恢复 manifest，并拒绝绝对路径、多层路径或缺失 checkpoint。
     *
     * @throws DatabaseValidationException identity、格式版本、路径或摘要不满足恢复不变量时抛出
     */
    public DdlBatchTableEntry {
        if (tableId == null || spaceId == null || relativePath == null
                || relativePath.isBlank() || rowFormatVersion <= 0
                || sourceSchemaDigest == null || pendingSchemaDigest == null
                || targetSchemaDigest == null) {
            throw new DatabaseValidationException(
                    "DDL batch table manifest fields are invalid");
        }
        try {
            Path path = Path.of(relativePath);
            if (path.isAbsolute() || path.getNameCount() != 1
                    || !path.getFileName().toString().equals(relativePath)
                    || relativePath.equals(".") || relativePath.equals("..")) {
                throw new DatabaseValidationException(
                        "DDL batch table path must be one controlled relative file name");
            }
        } catch (InvalidPathException invalidPath) {
            throw new DatabaseValidationException(
                    "DDL batch table path is invalid", invalidPath);
        }
    }
}
