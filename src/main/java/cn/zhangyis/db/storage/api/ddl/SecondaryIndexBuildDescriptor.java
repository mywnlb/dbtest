package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 既有表空间中一个未决二级索引构建的稳定物理所有权证据。
 *
 * @param ddlOperationId DDL log 的正 identity，用于拒绝另一条恢复任务回收本次资源
 * @param dictionaryVersion 本次 CREATE INDEX 预留且最终应发布的字典版本
 * @param tableId 既有 ACTIVE table 的稳定 identity
 * @param indexBinding 新索引 root 与 leaf/non-leaf segment 的完整物理绑定
 */
public record SecondaryIndexBuildDescriptor(long ddlOperationId, long dictionaryVersion,
                                            long tableId, IndexStorageBinding indexBinding) {

    public SecondaryIndexBuildDescriptor {
        if (ddlOperationId <= 0 || dictionaryVersion <= 0 || tableId <= 0 || indexBinding == null) {
            throw new DatabaseValidationException("secondary index build descriptor fields are invalid");
        }
    }
}
