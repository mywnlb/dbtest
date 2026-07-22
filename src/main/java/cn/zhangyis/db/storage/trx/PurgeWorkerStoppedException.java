package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 组合根已请求停止 purge pool；用于区分正常关闭取消和业务执行失败。 */
public final class PurgeWorkerStoppedException extends DatabaseRuntimeException {

    /** @param message 包含 pool 生命周期或记录 identity 的停止诊断 */
    public PurgeWorkerStoppedException(String message) {
        super(message);
    }

    /**
     * @param message 包含 pool 生命周期或记录 identity 的停止诊断
     * @param cause 触发 dispatcher 醒来的原始取消或中断
     */
    public PurgeWorkerStoppedException(String message, Throwable cause) {
        super(message, cause);
    }
}
