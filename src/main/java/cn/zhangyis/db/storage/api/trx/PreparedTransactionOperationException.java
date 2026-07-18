package cn.zhangyis.db.storage.api.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * prepared transaction phase 或 durability 屏障失败。调用方应依据事务当前状态重试相同 phase-two 决议；
 * PREPARED/PREPARED_ROLLING_BACK 与 terminal-but-not-acknowledged 状态都不会由本异常隐式改写为相反决议。
 */
public final class PreparedTransactionOperationException extends DatabaseRuntimeException {

    /**
     * 创建 {@code PreparedTransactionOperationException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public PreparedTransactionOperationException(String message) {
        super(message);
    }

    /**
     * 创建 {@code PreparedTransactionOperationException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public PreparedTransactionOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
