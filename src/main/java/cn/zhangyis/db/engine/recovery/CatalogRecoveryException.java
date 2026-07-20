package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 显式离线 catalog recovery 无法安全完成时的异常基类。
 */
public class CatalogRecoveryException extends DatabaseRuntimeException {

    /**
     * 创建带恢复阶段上下文的异常。
     *
     * @param message 说明 inspect/quarantine/rebuild 阶段和被破坏不变量的文本
     */
    public CatalogRecoveryException(String message) {
        super(message);
    }

    /**
     * 包装底层文件、manifest、control 或 catalog 失败并保留根因。
     *
     * @param message 说明 inspect/quarantine/rebuild 阶段和被破坏不变量的文本
     * @param cause 原始失败
     */
    public CatalogRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
