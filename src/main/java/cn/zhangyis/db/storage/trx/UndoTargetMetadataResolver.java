package cn.zhangyis.db.storage.trx;

/** rollback/recovery 根据 undo 固定 tableId/indexId 解析聚簇索引与权威 LOB segment 的内部 SPI。 */
@FunctionalInterface
public interface UndoTargetMetadataResolver {
    /** 未知、DROPPED、非聚簇索引或 binding 不一致必须抛领域异常，不允许回退最后使用的索引。
     *
     * @param tableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
     * @param indexId 参与 {@code resolveTarget} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code resolveTarget} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     */
    UndoTargetMetadata resolveTarget(long tableId, long indexId);
}
