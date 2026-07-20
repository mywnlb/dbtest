package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * DD catalog 缺失、未初始化或无法证明属于 fresh 实例时的致命启动异常。
 *
 * <p>该异常表示继续创建空 catalog 可能把已有表空间误判为 orphan 并删除。调用方只能关闭实例、
 * 恢复可信 {@code mysql.ibd}，或等待显式 catalog rebuild 工具处理；普通重试不得绕过该异常。</p>
 */
public final class DictionaryCatalogAdmissionException extends DatabaseFatalException {

    /**
     * 创建带完整 catalog 状态与持久证据上下文的准入异常。
     *
     * @param message 必须说明 catalog 状态、拒绝原因及安全恢复方向，不能暗示空 catalog 已经发布
     */
    public DictionaryCatalogAdmissionException(String message) {
        super(message);
    }

    /**
     * 创建保留底层文件系统或持久证据探测根因的准入异常。
     *
     * @param message 必须说明无法证明实例为 fresh，且后续 recovery/cleanup 尚未执行
     * @param cause 导致证据状态不可判定的原始异常；调用方可据此诊断权限、路径或 IO 故障
     */
    public DictionaryCatalogAdmissionException(String message, Throwable cause) {
        super(message, cause);
    }
}
