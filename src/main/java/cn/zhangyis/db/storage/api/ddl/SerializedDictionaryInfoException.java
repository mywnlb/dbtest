package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * SDI 页无法读取、校验或持久化时的稳定 storage API 异常。committed DD recovery 可选择重写逻辑 SDI；
 * 物理 page checksum/envelope 或未知 root 损坏则应让该异常阻止实例开放。
 */
public final class SerializedDictionaryInfoException extends DatabaseRuntimeException {

    /**
     * 创建只含稳定 storage API 诊断消息的 SDI 异常。
     *
     * @param message root、envelope、CRC、容量或持久化边界的领域描述
     */
    public SerializedDictionaryInfoException(String message) {
        super(message);
    }

    /**
     * 创建保留内部页格式、IO 或 flush 根因的 SDI 异常。
     *
     * @param message 面向 DD/recovery 的稳定领域描述
     * @param cause 触发失败的 storage 内部异常
     */
    public SerializedDictionaryInfoException(String message, Throwable cause) {
        super(message, cause);
    }
}
