package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间物理文件未打开异常。对未经 create/open 登记（或已 close）的 SpaceId 发起物理 IO。
 * 可恢复：调用方应先 open/create 对应表空间文件。
 */
public class TablespaceNotOpenException extends DatabaseRuntimeException {

    public TablespaceNotOpenException(String message) {
        super(message);
    }

    public TablespaceNotOpenException(String message, Throwable cause) {
        super(message, cause);
    }
}
