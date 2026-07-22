package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * Row-log immutable header 的领域快照。manifest 是 DD 编码的 opaque bytes；storage 只验证摘要并原样保存。
 */
public final class OnlineIndexLogHeader {

    private final OnlineIndexBuildId buildId;
    private final long tableId;
    private final long indexId;
    private final long sourceDictionaryVersion;
    private final long targetDictionaryVersion;
    private final long rowFormatVersion;
    private final byte[] manifest;

    /**
     * 创建完成交叉校验的 immutable header。
     *
     * @param buildId DDL/build 正身份
     * @param tableId 既有 ACTIVE table 正身份
     * @param indexId 本次新 secondary index 正身份
     * @param sourceDictionaryVersion 构建开始时 committed table version
     * @param targetDictionaryVersion 最终应发布且严格大于 source 的 version
     * @param rowFormatVersion 聚簇记录物理编码版本，CREATE INDEX 不改变它
     * @param manifest DD 完整 logical build 定义的非空稳定编码
     * @throws DatabaseValidationException identity/version/manifest 不能形成合法构建时抛出
     */
    public OnlineIndexLogHeader(OnlineIndexBuildId buildId, long tableId, long indexId,
                                long sourceDictionaryVersion, long targetDictionaryVersion,
                                long rowFormatVersion, byte[] manifest) {
        if (buildId == null || tableId <= 0 || indexId <= 0 || sourceDictionaryVersion <= 0
                || targetDictionaryVersion <= sourceDictionaryVersion || rowFormatVersion <= 0
                || manifest == null || manifest.length == 0) {
            throw new DatabaseValidationException("online index log header fields are invalid");
        }
        this.buildId = buildId;
        this.tableId = tableId;
        this.indexId = indexId;
        this.sourceDictionaryVersion = sourceDictionaryVersion;
        this.targetDictionaryVersion = targetDictionaryVersion;
        this.rowFormatVersion = rowFormatVersion;
        this.manifest = manifest.clone();
    }

    public OnlineIndexBuildId buildId() { return buildId; }
    public long tableId() { return tableId; }
    public long indexId() { return indexId; }
    public long sourceDictionaryVersion() { return sourceDictionaryVersion; }
    public long targetDictionaryVersion() { return targetDictionaryVersion; }
    public long rowFormatVersion() { return rowFormatVersion; }
    public byte[] manifest() { return manifest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof OnlineIndexLogHeader that)) return false;
        return tableId == that.tableId && indexId == that.indexId
                && sourceDictionaryVersion == that.sourceDictionaryVersion
                && targetDictionaryVersion == that.targetDictionaryVersion
                && rowFormatVersion == that.rowFormatVersion && buildId.equals(that.buildId)
                && Arrays.equals(manifest, that.manifest);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(buildId, tableId, indexId, sourceDictionaryVersion,
                targetDictionaryVersion, rowFormatVersion);
        return 31 * result + Arrays.hashCode(manifest);
    }
}
