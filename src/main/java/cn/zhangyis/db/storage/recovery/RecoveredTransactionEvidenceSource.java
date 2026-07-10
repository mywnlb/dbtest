package cn.zhangyis.db.storage.recovery;

/** 恢复事务证据来源；用于区分 redo terminal 与 checkpoint 覆盖下接受的 page3 状态。 */
public enum RecoveredTransactionEvidenceSource {
    /** checkpoint 后的事务状态 logical redo。 */
    REDO,
    /** redo replay 后读取的 rollback-segment page3/undo first-page header。 */
    PAGE3
}
