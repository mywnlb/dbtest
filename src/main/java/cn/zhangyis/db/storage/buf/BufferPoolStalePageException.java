package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Buffer Pool 拒绝返回陈旧页时抛出的领域异常。
 *
 * <p>典型场景是 truncate/drop/discard 已经打开表空间维护窗口，或 LOADING owner 读盘完成时发现表空间版本已推进。
 * 调用方可在释放当前 MTR/资源后重新定位或等待维护完成；异常不表示磁盘损坏。
 */
public final class BufferPoolStalePageException extends DatabaseRuntimeException {

    public BufferPoolStalePageException(String message) {
        super(message);
    }

    public BufferPoolStalePageException(String message, Throwable cause) {
        super(message, cause);
    }
}
