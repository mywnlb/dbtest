package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 发布前拦截器在取得 pending page 写能力后失败。此时页面内容或配套持久状态可能已改变，Buffer Pool 不能把
 * frame 复位重用，也不能向等待者发布旧/半合并内容；引擎必须 fail-stop 并由 crash recovery 重建一致状态。
 */
public final class PagePublicationFatalException extends DatabaseFatalException {

    /**
     * 创建没有更底层异常、但已经破坏发布协议不变量的致命错误。
     *
     * @param message 包含目标 PageId 与失败阶段的诊断信息
     */
    public PagePublicationFatalException(String message) {
        super(message);
    }

    /**
     * @param message 包含目标 PageId 与失败阶段的诊断信息
     * @param cause 拦截器原始异常
     */
    public PagePublicationFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
