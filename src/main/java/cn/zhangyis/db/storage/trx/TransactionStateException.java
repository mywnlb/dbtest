package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 事务状态/生命周期非法操作异常：非法状态转换、对只读事务分配写 id、对已结束（COMMITTED/ROLLED_BACK）
 * 或提交中事务再发起操作。调用方可据此回滚或上报，不应静默吞掉。
 */
public class TransactionStateException extends DatabaseRuntimeException {

    /**
     * 创建 {@code TransactionStateException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public TransactionStateException(String message) {
        super(message);
    }

    /**
     * 创建 {@code TransactionStateException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public TransactionStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
