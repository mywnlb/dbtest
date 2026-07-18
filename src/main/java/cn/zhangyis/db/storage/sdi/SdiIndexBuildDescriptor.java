package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;

/**
 * page3 footer 的物理视图。该对象不表达 DD 提交状态，只证明一组未决 index segment/root 属于哪条 DDL。
 *
 * @param ddlOperationId DDL identity
 * @param dictionaryVersion 目标字典版本
 * @param tableId 表 identity
 * @param indexBinding 新索引物理绑定
 */
public record SdiIndexBuildDescriptor(long ddlOperationId, long dictionaryVersion,
                                      long tableId, IndexStorageBinding indexBinding) {

    public SdiIndexBuildDescriptor {
        if (ddlOperationId <= 0 || dictionaryVersion <= 0 || tableId <= 0 || indexBinding == null) {
            throw new DatabaseValidationException("SDI index build descriptor fields are invalid");
        }
    }
}
