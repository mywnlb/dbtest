package cn.zhangyis.db.sql.optimizer.physical;

/**
 * Nested Loop Join 右输入的重开策略。
 *
 * <ul>
 *     <li>{@link #FULL_SCAN}：每个 outer row 重开无界聚簇扫描。</li>
 *     <li>{@link #POINT}：用 outer key 构造聚簇或唯一二级点探针。</li>
 *     <li>{@link #SECONDARY_PREFIX}：用 outer key 构造普通单列二级前缀探针。</li>
 * </ul>
 */
public enum PhysicalJoinProbeKind {
    FULL_SCAN,
    POINT,
    SECONDARY_PREFIX
}
