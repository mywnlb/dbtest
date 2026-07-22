package cn.zhangyis.db.storage.api.undotruncate;

/** stable undo space reusable-owner drain 的包内结果；非 DRAINED 状态均未开始物理 owner 修改。 */
enum UndoReusableSegmentDrainStatus {
    /** cache/free owner 已全部持久回收，page3 reusable 集合为空。 */
    DRAINED,
    /** page3 history 非空。 */
    DEFERRED_HISTORY,
    /** page3 active slot 非空。 */
    DEFERRED_ACTIVE_SLOTS,
    /** 另一 reuse transition 已跨出短锁。 */
    DEFERRED_REUSE_BUSY
}
