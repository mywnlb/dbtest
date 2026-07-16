package cn.zhangyis.db.storage.btree;

/** rollback/purge 根据 undo 固定前缀身份解析不可变 BTreeIndex 快照的内部 SPI。 */
@FunctionalInterface
public interface IndexMetadataResolver {
    /** 未知、已删除或 binding 不一致必须抛领域异常，禁止回退某个全局索引。 */
    BTreeIndex resolve(long tableId, long indexId);
}
