package cn.zhangyis.db.storage.recovery;

/** undo first-page header 在当前教学布局中可恢复的两种稳定状态。 */
public enum RecoveredUndoState {
    /** 事务崩溃时尚未完成，必须进入 recovered-active rollback。 */
    ACTIVE,
    /** 事务已提交，undo 段应重建到 history list。 */
    COMMITTED
}
