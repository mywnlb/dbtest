package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** ticket 已关闭、升级模式非法或 owner/key 不一致等 MDL 生命周期错误。 */
public class MetadataLockStateException extends DatabaseRuntimeException {
    public MetadataLockStateException(String message) {
        super(message);
    }

    public MetadataLockStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
