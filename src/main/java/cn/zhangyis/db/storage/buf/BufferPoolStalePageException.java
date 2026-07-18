package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Buffer Pool 拒绝返回陈旧页时抛出的领域异常。
 *
 * <p>典型场景是 truncate/drop/discard 已经打开表空间维护窗口，或 LOADING owner 读盘完成时发现表空间版本已推进。
 * 调用方可在释放当前 MTR/资源后重新定位或等待维护完成；异常不表示磁盘损坏。
 */
public final class BufferPoolStalePageException extends DatabaseRuntimeException {

    /**
     * 创建 {@code BufferPoolStalePageException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BufferPoolStalePageException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BufferPoolStalePageException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BufferPoolStalePageException(String message, Throwable cause) {
        super(message, cause);
    }
}
