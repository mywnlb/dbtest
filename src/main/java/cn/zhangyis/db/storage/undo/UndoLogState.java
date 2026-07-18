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

    /**
     * 根据调用参数构造 {@code fromPhysical} 对应的Undo 日志领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code fromPhysical} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
