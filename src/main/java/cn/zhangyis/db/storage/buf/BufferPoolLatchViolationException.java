package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Buffer Pool 内部锁边界被破坏时抛出的领域异常。
 *
 * <p>该异常表示实现代码在持有 Buffer Pool 内部锁时进入了物理 IO、载入 future 等待或脏页淘汰 flush
 * 等可能阻塞路径。它不是用户数据损坏，而是内核并发不变量违规；调用方通常只能让当前操作失败并暴露诊断上下文。
 */
public final class BufferPoolLatchViolationException extends DatabaseRuntimeException {

    /**
     * 创建 {@code BufferPoolLatchViolationException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BufferPoolLatchViolationException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BufferPoolLatchViolationException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BufferPoolLatchViolationException(String message, Throwable cause) {
        super(message, cause);
    }
}
