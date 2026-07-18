package cn.zhangyis.db.storage.flush;

/** 批量刷脏的候选来源：按 checkpoint 顺序或按 LRU 腾挪容量。 */
public enum FlushBatchSource {
    /** 按 oldest modification LSN 推进 fuzzy checkpoint。 */
    FLUSH_LIST,
    /** 按 LRU 尾部刷出 dirty victim，优先释放可复用 frame。 */
    LRU
}
