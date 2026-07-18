package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** undo 表空间仍有已分配 inode/segment，说明 purge 尚未完成，不能物理截断。 */
public final class UndoTablespaceNotEmptyException extends DatabaseRuntimeException {

    /**
     * 创建 {@code UndoTablespaceNotEmptyException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UndoTablespaceNotEmptyException(String message) {
        super(message);
    }
}
