package cn.zhangyis.db.storage.trx;

/** rollback/recovery 根据 undo 固定 tableId/indexId 解析聚簇索引与权威 LOB segment 的内部 SPI。 */
@FunctionalInterface
public interface UndoTargetMetadataResolver {
    /** 未知、DROPPED、非聚簇索引或 binding 不一致必须抛领域异常，不允许回退最后使用的索引。 */
    UndoTargetMetadata resolveTarget(long tableId, long indexId);
}
