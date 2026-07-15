package cn.zhangyis.db.storage.redo;

/**
 * Undo/rseg metadata delta 的稳定磁盘分类。分类只用于恢复期校验、审计和诊断；真正恢复动作仍是把
 * {@link UndoMetadataDeltaRecord#afterImage()} 覆盖到指定页内 offset，不能重新执行事务提交、slot 分配或 undo append
 * 状态机。
 */
public enum UndoMetadataDeltaKind {

    /** rollback segment header page3 的固定头字段，例如 magic、format、rsegId、slotCapacity。 */
    RSEG_HEADER_FIELD((byte) 1),
    /** rollback segment header page3 的 slot 项，payload 为 slot 登记的 first page no 或 FIL_NULL。 */
    RSEG_SLOT((byte) 2),
    /** undo 页通用 page header 字段，例如 freeOffset、recordCount、segmentId、inodeSlot、pageFlags。 */
    UNDO_PAGE_HEADER_FIELD((byte) 3),
    /** undo first 页 log header 字段，例如 transactionId、state、lastPageNo、commitNo。 */
    UNDO_LOG_HEADER_FIELD((byte) 4),
    /** undo 页 FIL sibling link 字段，当前用于链页 prev/next 关系的后续逻辑化预留。 */
    UNDO_FIL_LINK_FIELD((byte) 5),
    /** rollback segment page3 v2 起的 INSERT/UPDATE cached segment pageNo 栈项。 */
    RSEG_CACHE_ENTRY((byte) 6),
    /** rollback segment page3 v2 起的 cached stack count。 */
    RSEG_CACHE_COUNT((byte) 7),
    /** rollback segment page3 v3 的持久 history base 字段。 */
    RSEG_HISTORY_BASE((byte) 8),
    /** undo first page v3 的 history prev/next 链接字段。 */
    UNDO_HISTORY_LINK_FIELD((byte) 9);

    /** redo 文件中的稳定 1 字节分类码；只能追加，不能重排。 */
    private final byte code;

    UndoMetadataDeltaKind(byte code) {
        this.code = code;
    }

    /** 返回 redo 文件中的稳定分类码。 */
    public byte code() {
        return code;
    }

    /**
     * 从 redo payload 的稳定分类码还原枚举。未知分类码说明 redo 文件损坏，恢复必须 fail-closed。
     *
     * @param code redo 文件中的 1 字节分类码。
     * @return 对应分类。
     */
    public static UndoMetadataDeltaKind fromCode(byte code) {
        for (UndoMetadataDeltaKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        throw new RedoLogCorruptedException("unknown undo metadata delta kind code: " + code);
    }
}
