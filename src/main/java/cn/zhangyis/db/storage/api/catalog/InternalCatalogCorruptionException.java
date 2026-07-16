package cn.zhangyis.db.storage.api.catalog;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 内部 catalog 物理格式或 durable 提交边界无法安全解释。该异常属于稳定 storage API，
 * 因此物理实现不需要反向依赖具体 DD repository 的异常层次。
 */
public class InternalCatalogCorruptionException extends DatabaseFatalException {

    public InternalCatalogCorruptionException(String message) {
        super(message);
    }

    public InternalCatalogCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
