package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 外部 undo payload 超过实例显式页数上限；规划阶段抛出，尚未进入任何物理页修改。 */
public final class UndoPayloadTooLargeException extends DatabaseRuntimeException {

    /** 创建只含配置/实际页数诊断的可恢复规划异常。 */
    public UndoPayloadTooLargeException(String message) {
        super(message);
    }

    /** 创建保留底层编码或算术根因的规划异常。 */
    public UndoPayloadTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }
}
