package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 既有表空间中一个未决二级索引删除的稳定物理所有权证据。
 *
 * <p>descriptor 在 DD 删除 index 前写入，DD commit 后才允许按其中 identity 回收 segment；恢复必须同时核对
 * marker、当前 DD 与本对象，不能仅凭 index id 释放 inode。</p>
 *
 * @param ddlOperationId DDL log 的正 identity
 * @param dictionaryVersion 删除索引后 table aggregate 的目标字典版本
 * @param tableId 既有 ACTIVE table 的稳定 identity
 * @param indexBinding 待回收索引的 root 与 leaf/non-leaf segment 完整绑定
 */
public record SecondaryIndexDropDescriptor(long ddlOperationId,
                                           long dictionaryVersion,
                                           long tableId,
                                           IndexStorageBinding indexBinding) {

    public SecondaryIndexDropDescriptor {
        if (ddlOperationId <= 0 || dictionaryVersion <= 0 || tableId <= 0 || indexBinding == null) {
            throw new DatabaseValidationException("secondary index drop descriptor fields are invalid");
        }
    }
}
