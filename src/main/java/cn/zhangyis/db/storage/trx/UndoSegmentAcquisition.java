package cn.zhangyis.db.storage.trx;

/**
 * 一次 undo append 获得目标 segment 的方式。它区分“新建物理 segment”和“从持久 cache 开启新逻辑 log”，
 * 避免旧 {@code newLog} 布尔值同时承担事务 binding 与 FSP allocation 两种含义。
 */
public enum UndoSegmentAcquisition {
    /** 目标 kind 尚无 binding 且没有稳定 cached top，需要新建 FSP segment。 */
    ALLOCATE_NEW,
    /** 目标 kind 尚无 binding，复用 page3 cached 栈顶的现有单页 FSP segment。 */
    REUSE_CACHED,
    /** 目标 kind 尚无 binding，复用 page3 free FIFO 队首并在激活时重新分类。 */
    REUSE_FREE,
    /** 事务已经拥有该 kind 的 ACTIVE segment，向现有尾页追加。 */
    APPEND_EXISTING;

    /** 是否为当前事务开启一条新的 kind-local undo log。 */
    public boolean startsNewLogicalLog() {
        return this != APPEND_EXISTING;
    }
}
