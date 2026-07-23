package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 已持久化 INSERT 批次超过目标 leaf 的保守连续空间。说明 bitmap/容量预留与真实页分叉，不能发布未完整合并页；
 * 引擎必须 fail-stop，保留 redo/global mutation 供恢复诊断。
 */
public final class ChangeBufferMergeCapacityException extends DatabaseFatalException {

    /** @param message 包含目标页、所需字节与实际 free space 的诊断信息 */
    public ChangeBufferMergeCapacityException(String message) {
        super(message);
    }
}
