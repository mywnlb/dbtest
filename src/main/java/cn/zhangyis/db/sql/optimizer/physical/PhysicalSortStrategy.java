package cn.zhangyis.db.sql.optimizer.physical;

/**
 * Optimizer 选定的排序执行策略。该枚举是 explain/test 可观察决策，不持有运行期堆或临时文件。
 */
public enum PhysicalSortStrategy {
    /** SQL 没有 ORDER BY。 */
    NONE,
    /** 已选访问索引的物理顺序满足完整 ORDER BY，不创建 SortNode。 */
    INDEX,
    /** LIMIT 上界较小时用最大堆仅保留 offset+count 个最佳候选。 */
    TOP_N_HEAP,
    /** 先形成多个内存有序 run，再通过最小堆稳定归并。 */
    PARTITIONED_HEAP_MERGE
}
