package cn.zhangyis.db.engine.recovery;

/**
 * 实例文件锁在有界等待内不可获得或等待线程被中断时抛出。
 */
public final class CatalogRecoveryBusyException extends CatalogRecoveryException {

    /**
     * @param message 包含实例目录与 timeout/interrupt 事实的诊断文本
     */
    public CatalogRecoveryBusyException(String message) {
        super(message);
    }

    /**
     * @param message 包含实例目录与底层锁失败上下文的诊断文本
     * @param cause 原始文件锁异常
     */
    public CatalogRecoveryBusyException(String message, Throwable cause) {
        super(message, cause);
    }
}
