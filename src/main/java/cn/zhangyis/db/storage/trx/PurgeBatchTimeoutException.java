package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** purge worker 批次未在配置上限内收口；pool 会 fail-stop，调用方不得直接启动下一批。 */
public final class PurgeBatchTimeoutException extends DatabaseRuntimeException {

    /**
     * 创建保留底层等待首因的批次超时异常。
     *
     * @param message 包含批次上限的诊断消息
     * @param cause completion 等待报告的原始超时
     */
    public PurgeBatchTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
