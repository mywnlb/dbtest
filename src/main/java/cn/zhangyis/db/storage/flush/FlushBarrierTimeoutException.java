package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 无法在超时内把 redo/dirty/checkpoint 安全边界推进到目标 LSN；生命周期操作应保持可恢复中间态。 */
public final class FlushBarrierTimeoutException extends DatabaseRuntimeException {

    public FlushBarrierTimeoutException(String message) {
        super(message);
    }
}
