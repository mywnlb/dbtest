package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** page0 SDI root 或 page3 逻辑格式无法安全解释时的内部异常，由 storage facade 转成稳定 API 异常。 */
public final class SdiPageCorruptionException extends DatabaseRuntimeException {

    /**
     * 创建只含物理 SDI 诊断消息的异常。
     *
     * @param message root、envelope、format、identity 或 CRC 错误
     */
    public SdiPageCorruptionException(String message) {
        super(message);
    }

    /**
     * 创建保留底层页读取或格式转换根因的异常。
     *
     * @param message 物理 SDI 错误描述
     * @param cause 原始异常
     */
    public SdiPageCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
