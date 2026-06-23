package cn.zhangyis.db.storage.btree;

/**
 * 聚簇记录物理删除结果（T1.3d）。{@code deleteClustered} 是幂等的：未命中、所有权（DB_TRX_ID/DB_ROLL_PTR）
 * 不匹配都返回 {@code removed=false} 而非抛异常，使 rollback 反向走链在 orphan undo / 重试场景下安全收敛。
 *
 * @param removed 是否真正摘除了一条匹配记录；{@code false} 表示「未命中或非本 undo 插入的行，未做任何修改」。
 */
public record BTreeDeleteResult(boolean removed) {
}
