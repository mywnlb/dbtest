package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** undo 表空间仍有已分配 inode/segment，说明 purge 尚未完成，不能物理截断。 */
public final class UndoTablespaceNotEmptyException extends DatabaseRuntimeException {

    public UndoTablespaceNotEmptyException(String message) {
        super(message);
    }
}
