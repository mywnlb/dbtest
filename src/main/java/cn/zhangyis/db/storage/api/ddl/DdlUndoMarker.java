package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * DDL 与 dictionary-row undo/恢复日志共享的稳定关联标记。它只携带跨模块数值身份，不让 storage 反向依赖 DD 类型。
 *
 * @param ddlOperationId   control durable 分配且跨 crash 不复用的 DDL identity。
 * @param dictionaryVersion 本次 DDL 的首个字典提交版本。
 * @param affectedObjectId 受影响 table 等字典聚合根的稳定 identity。
 */
public record DdlUndoMarker(long ddlOperationId, long dictionaryVersion, long affectedObjectId) {

    /**
     * 保证 marker 创建后始终能唯一关联一条 DDL、一个字典版本和一个对象。
     *
     * @throws DatabaseValidationException 任一 identity 非正时抛出，调用方不得写入 DDL log。
     */
    public DdlUndoMarker {
        if (ddlOperationId <= 0 || dictionaryVersion <= 0 || affectedObjectId <= 0) {
            throw new DatabaseValidationException("DDL undo marker identities must be positive");
        }
    }
}
