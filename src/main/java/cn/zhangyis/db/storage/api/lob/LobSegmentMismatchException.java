package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 调用方把 LOB 操作交给非 LOB、陈旧或错误 inode segment；在任何 payload 页修改前拒绝。 */
public class LobSegmentMismatchException extends DatabaseRuntimeException {
    public LobSegmentMismatchException(String message) {
        super(message);
    }

    public LobSegmentMismatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
