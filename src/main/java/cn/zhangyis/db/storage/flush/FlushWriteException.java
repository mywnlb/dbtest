package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Flush/doublewrite/data-file 写盘路径的可恢复运行时异常。调用方不得把失败页标 clean，
 * 可以保留 dirty 并在后续 flush 轮次重试，或把错误上报给恢复/只读降级流程。
 */
public class FlushWriteException extends DatabaseRuntimeException {

    /**
     * 创建 {@code FlushWriteException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public FlushWriteException(String message) {
        super(message);
    }

    /**
     * 创建 {@code FlushWriteException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public FlushWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
