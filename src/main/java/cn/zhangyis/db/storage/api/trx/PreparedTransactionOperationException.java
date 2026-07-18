package cn.zhangyis.db.storage.api.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * prepared transaction phase 或 durability 屏障失败。调用方应依据事务当前状态重试相同 phase-two 决议；
 * PREPARED/PREPARED_ROLLING_BACK 与 terminal-but-not-acknowledged 状态都不会由本异常隐式改写为相反决议。
 */
public final class PreparedTransactionOperationException extends DatabaseRuntimeException {

    public PreparedTransactionOperationException(String message) {
        super(message);
    }

    public PreparedTransactionOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
