package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 数据文件操作无法完成的可恢复物理层异常。
 *
 * <p>触发范围包括文件已存在/不存在、create/open/close、positional 页读写、unexpected EOF、
 * 零填充扩展、truncate 和 {@code force} 失败。包装 {@link java.io.IOException} 时必须保留 cause；
 * {@code forceAll}/{@code close()} 聚合多个文件失败时，其余失败作为 suppressed 异常保留。</p>
 *
 * <p>该异常不承诺失败操作没有产生部分物理效果：写页、扩展、截断或 force 的调用方必须保留
 * dirty/recovery 状态，不能仅凭异常清除待处理工作。是否可重试、关闭实例或转入恢复，由上层根据
 * 操作阶段决定。</p>
 */
public class DataFilePhysicalException extends DatabaseRuntimeException {

    /**
     * 创建不依赖底层 Throwable 的物理失败异常。
     *
     * @param message 应包含文件路径、SpaceId、页偏移或失败操作等可定位上下文
     */
    public DataFilePhysicalException(String message) {
        super(message);
    }

    /**
     * 创建保留底层 IO 根因的物理失败异常。
     *
     * @param message 应包含文件路径及失败操作阶段
     * @param cause 原始 IO/文件系统异常；不得丢弃
     */
    public DataFilePhysicalException(String message, Throwable cause) {
        super(message, cause);
    }
}
