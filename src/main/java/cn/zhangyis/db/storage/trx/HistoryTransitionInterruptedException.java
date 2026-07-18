package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** history transition 等待被中断；构造前调用方已恢复线程中断标志，并保留 InterruptedException 根因。 */
public final class HistoryTransitionInterruptedException extends DatabaseRuntimeException {

    /**
     * 创建 {@code HistoryTransitionInterruptedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public HistoryTransitionInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
