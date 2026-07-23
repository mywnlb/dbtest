package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/** Change Buffer header、record 或 bitmap 持久证据损坏，调用方不得猜测修复或跳过。 */
public final class ChangeBufferFormatException extends DatabaseFatalException {

    /**
     * 创建没有更底层根因的持久格式矛盾。
     *
     * @param message 包含损坏字段、PageId 或跨证据不一致上下文的诊断描述
     */
    public ChangeBufferFormatException(String message) {
        super(message);
    }

    /**
     * 创建保留底层解码、页访问或校验根因的持久格式矛盾。
     *
     * @param message 包含损坏字段、PageId 或跨证据不一致上下文的诊断描述
     * @param cause 触发格式校验失败的原始异常；不得丢失
     */
    public ChangeBufferFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
