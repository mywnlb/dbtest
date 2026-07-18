package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 等待某页载入（LOADING）完成超时或被中断。表达"有界等待"约束：命中正在载入的页时，等待者不会无限期阻塞，
 * 到达配置的 load 超时或被中断即抛出本异常，调用方可重试或上报，绝不悬挂（设计 §7.3 IO owner 完成/失败语义）。
 */
public final class BufferPoolLoadTimeoutException extends DatabaseRuntimeException {

    /**
     * 创建 {@code BufferPoolLoadTimeoutException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BufferPoolLoadTimeoutException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BufferPoolLoadTimeoutException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BufferPoolLoadTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
