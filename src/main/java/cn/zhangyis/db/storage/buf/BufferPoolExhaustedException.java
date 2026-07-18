package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Buffer Pool 帧耗尽异常。需要新帧但所有帧都被 fix（无可淘汰受害者）时抛出。
 * 可恢复：调用方释放 PageGuard 后可重试。
 */
public class BufferPoolExhaustedException extends DatabaseRuntimeException {

    /**
     * 创建 {@code BufferPoolExhaustedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BufferPoolExhaustedException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BufferPoolExhaustedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BufferPoolExhaustedException(String message, Throwable cause) {
        super(message, cause);
    }
}
