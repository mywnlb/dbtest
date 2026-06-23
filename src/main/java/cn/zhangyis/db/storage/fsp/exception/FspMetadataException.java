package cn.zhangyis.db.storage.fsp.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * fsp 元数据约束异常（设计 §17 风格）：extent 超出 page 0 首批 XDES 区、普通路径误用 extent0、坏枚举 ordinal、
 * nextSegmentId 破坏 0 哨兵不变量、读空 inode 槽、无空 inode/fragment 槽等。
 * 可恢复运行时异常。
 */
public class FspMetadataException extends DatabaseRuntimeException {

    public FspMetadataException(String message) {
        super(message);
    }

    public FspMetadataException(String message, Throwable cause) {
        super(message, cause);
    }
}
