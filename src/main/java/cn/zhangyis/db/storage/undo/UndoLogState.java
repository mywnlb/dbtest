package cn.zhangyis.db.storage.undo;

/** undo first-page log header 的领域状态；隔离页内物理编码，恢复层不依赖裸整数。 */
public enum UndoLogState {
    /** 事务仍可能回滚，history links 必须为空。 */
    ACTIVE,
    /** XA phase one 已持久化，仍有事务 owner且不得进入 history/purge。 */
    PREPARED,
    /** UPDATE undo 已提交并且必须出现在持久 history 链中。 */
    COMMITTED,
    /** 无事务 owner、由 page3 cache stack 持有的空单页 segment。 */
    CACHED,
    /** 无事务 owner、由 page3 free FIFO 持有且可跨 kind 复用的空单页 segment。 */
    FREE;

    static UndoLogState fromPhysical(int state) {
        return switch (state) {
            case UndoPageLayout.STATE_ACTIVE -> ACTIVE;
            case UndoPageLayout.STATE_PREPARED -> PREPARED;
            case UndoPageLayout.STATE_COMMITTED -> COMMITTED;
            case UndoPageLayout.STATE_CACHED -> CACHED;
            case UndoPageLayout.STATE_FREE -> FREE;
            default -> throw new UndoLogFormatException("unknown undo log physical state: " + state);
        };
    }
}
