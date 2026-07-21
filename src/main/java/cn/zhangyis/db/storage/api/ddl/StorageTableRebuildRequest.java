package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 一次阻塞式 shadow rebuild 的完整源/目标 schema 与列投影。
 *
 * @param sourceDefinition committed 源 row format
 * @param sourceBinding committed 源物理 binding
 * @param targetDefinition 尚未发布、使用新 space/version 的目标定义
 * @param rewrites 与 target columns 同 ordinal 的投影
 */
public record StorageTableRebuildRequest(StorageTableDefinition sourceDefinition,
                                         TableStorageBinding sourceBinding,
                                         StorageTableDefinition targetDefinition,
                                         List<StorageColumnRewrite> rewrites) {
    public StorageTableRebuildRequest {
        if (sourceDefinition == null || sourceBinding == null || targetDefinition == null
                || rewrites == null
                || sourceDefinition.tableId() != sourceBinding.tableId()
                || sourceDefinition.tableId() != targetDefinition.tableId()
                || sourceDefinition.spaceId().equals(targetDefinition.spaceId())
                || rewrites.size() != targetDefinition.columns().size()) {
            throw new DatabaseValidationException("storage table rebuild request is invalid");
        }
        rewrites = List.copyOf(rewrites);
        for (StorageColumnRewrite rewrite : rewrites) {
            if (rewrite.sourceOrdinal() >= sourceDefinition.columns().size()) {
                throw new DatabaseValidationException(
                        "storage rebuild source ordinal is outside source schema");
            }
        }
    }
}
