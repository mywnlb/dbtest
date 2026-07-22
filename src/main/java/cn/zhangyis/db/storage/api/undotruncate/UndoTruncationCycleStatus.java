package cn.zhangyis.db.storage.api.undotruncate;

/** 自动 truncate scheduler 最近一个可观察 cycle 的状态。 */
public enum UndoTruncationCycleStatus {
    /** 已启用但尚未到达第一次检查。 */
    NEVER_RUN,
    /** 配置关闭自动调度。 */
    DISABLED,
    /** 检查完成但增长不足 extent 门槛。 */
    BELOW_THRESHOLD,
    /** 零等待 lifecycle X lease 竞争失败。 */
    DEFERRED_ACCESS_BUSY,
    /** page3 history 非空。 */
    DEFERRED_HISTORY,
    /** page3 active slot 非空。 */
    DEFERRED_ACTIVE_SLOTS,
    /** reuse owner transition busy。 */
    DEFERRED_REUSE_BUSY,
    /** crash-safe truncate 已完成或幂等续作完成。 */
    COMPLETED,
    /** 非 deferred 存储错误已传播给 purge driver。 */
    FAILED
}
