package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 离线 full-page scrub 无法证明候选表空间完整时抛出的领域异常。
 *
 * <p>该异常只报告只读扫描失败，不授权修页、跳页或隔离 manifest 唯一期望文件。调用方应保留原文件，
 * 把失败纳入 catalog recovery conflict，并从备份或其它明确证据恢复。</p>
 */
public final class TablespaceScrubException extends DatabaseRuntimeException {

    /**
     * 创建带扫描阶段上下文的 scrub 异常。
     *
     * @param message 包含路径、页号或被破坏不变量的诊断文本
     */
    public TablespaceScrubException(String message) {
        super(message);
    }

    /**
     * 包装底层只读 IO、属性读取或解码失败并保留根因。
     *
     * @param message 包含路径、页号或被破坏不变量的诊断文本
     * @param cause 原始失败；不得丢弃
     */
    public TablespaceScrubException(String message, Throwable cause) {
        super(message, cause);
    }
}
