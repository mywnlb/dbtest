package cn.zhangyis.db.storage.recovery;

/** page3 occupied owner 对应的 undo first-page 可恢复稳定状态；CACHED/FREE 由独立目录恢复。 */
public enum RecoveredUndoState {
    /** 事务崩溃时尚未完成，必须进入 recovered-active rollback。 */
    ACTIVE,
    /** XA phase one 已持久化，必须等待或消费上层协调器决议，不能按 ACTIVE 自动回滚。 */
    PREPARED,
    /** 事务已提交，undo 段应重建到 history list。 */
    COMMITTED
}
