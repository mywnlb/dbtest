package cn.zhangyis.db.storage.btree;

/** rollback/purge 根据 undo 固定前缀身份解析不可变 BTreeIndex 快照的内部 SPI。 */
@FunctionalInterface
public interface IndexMetadataResolver {
    /** 未知、已删除或 binding 不一致必须抛领域异常，禁止回退某个全局索引。
     *
     * @param tableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
     * @param indexId 参与 {@code resolve} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code resolve} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    BTreeIndex resolve(long tableId, long indexId);
}
