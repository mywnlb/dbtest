package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;

/**
 * page3 footer 的通用索引 DDL 物理所有权视图。该对象不表达 DD 是否提交，只把恢复动作与精确 root/segment
 * identity 绑定，防止旧恢复任务清理另一条 DDL 的资源。
 *
 * @param action BUILD 或 DROP；v1 持久数据只会解码为 BUILD
 * @param ddlOperationId DDL log 的正 identity
 * @param dictionaryVersion 本次 DDL 预留的目标字典版本
 * @param tableId 既有表的稳定 identity
 * @param indexBinding 本次构建或删除的完整索引物理绑定
 */
public record SdiIndexDdlDescriptor(SdiIndexDdlAction action,
                                    long ddlOperationId,
                                    long dictionaryVersion,
                                    long tableId,
                                    IndexStorageBinding indexBinding) {

    public SdiIndexDdlDescriptor {
        if (action == null || ddlOperationId <= 0 || dictionaryVersion <= 0
                || tableId <= 0 || indexBinding == null) {
            throw new DatabaseValidationException("SDI index DDL descriptor fields are invalid");
        }
    }
}
