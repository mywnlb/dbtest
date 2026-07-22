package cn.zhangyis.db.storage.api.undotruncate;

/** 自动 undo truncate 单次尝试的稳定结果分类；deferred 均发生在 marker 和物理副作用之前。 */
public enum UndoTruncationAttemptStatus {
    /** 文件相对持久 initial size 的增长不足配置门槛。 */
    BELOW_THRESHOLD,
    /** 普通表空间访问或已排队 owner 使零等待 X lease 未取得。 */
    DEFERRED_ACCESS_BUSY,
    /** page3 持久 history 仍引用当前 undo space。 */
    DEFERRED_HISTORY,
    /** page3 仍存在 active/prepared transaction slot。 */
    DEFERRED_ACTIVE_SLOTS,
    /** cache/free owner transition 已跨出短锁，当前 cycle 不能取得全局 drain gate。 */
    DEFERRED_REUSE_BUSY,
    /** 已完成或幂等续作完整 crash-safe truncate 生命周期。 */
    COMPLETED;

    /**
     * 判断结果是否是可重试并发/生命周期条件，而不是阈值跳过或成功。
     *
     * @return 四种 deferred 状态返回 {@code true}
     */
    public boolean deferred() {
        return this == DEFERRED_ACCESS_BUSY || this == DEFERRED_HISTORY
                || this == DEFERRED_ACTIVE_SLOTS || this == DEFERRED_REUSE_BUSY;
    }
}
