package cn.zhangyis.db.sql.optimizer.physical;

/**
 * SELECT 物理树的存储访问叶。访问叶只缩小候选集合，不携带最终 residual 或公开投影，
 * 从而保证 Filter/Project 可以映射为独立 Executor 算子。
 */
public sealed interface PhysicalAccess extends PhysicalOperator permits PhysicalPointAccess,
        PhysicalSecondaryPrefixAccess, PhysicalRangeAccess {

    /**
     * 返回 Optimizer 已选访问索引的稳定 DD 身份。
     *
     * @return 属于 {@link #table()} exact version 的正 index id
     */
    long accessIndexId();
}
