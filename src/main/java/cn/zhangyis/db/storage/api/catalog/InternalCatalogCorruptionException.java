package cn.zhangyis.db.storage.api.catalog;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 内部 catalog 物理格式或 durable 提交边界无法安全解释。该异常属于稳定 storage API，
 * 因此物理实现不需要反向依赖具体 DD repository 的异常层次。
 */
public class InternalCatalogCorruptionException extends DatabaseFatalException {

    /**
     * 创建 {@code InternalCatalogCorruptionException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public InternalCatalogCorruptionException(String message) {
        super(message);
    }

    /**
     * 创建 {@code InternalCatalogCorruptionException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public InternalCatalogCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
