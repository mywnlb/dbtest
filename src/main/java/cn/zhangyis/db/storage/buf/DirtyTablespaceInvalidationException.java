package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 截断排空发现目标表空间仍有脏帧；调用方必须先完成 WAL-safe flush，不能丢弃脏数据。 */
public final class DirtyTablespaceInvalidationException extends DatabaseRuntimeException {

    public DirtyTablespaceInvalidationException(String message) {
        super(message);
    }
}
