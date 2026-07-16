package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;

/** storage API 内把稳定 DDL DTO/binding 组装成 BTreeIndex 运行期快照的唯一工厂。 */
public final class BTreeIndexMetadataFactory {

    private final StorageTableSchemaMapper mapper = new StorageTableSchemaMapper();

    /** 逻辑 indexId、物理 binding 与 table schema 必须精确一致。 */
    public BTreeIndex create(StorageTableDefinition table, StorageIndexDefinition index,
                             IndexStorageBinding binding) {
        if (table == null || index == null || binding == null || index.indexId() != binding.indexId()
                || !binding.rootPageId().spaceId().equals(table.spaceId())) {
            throw new DatabaseValidationException("BTree metadata factory input identity mismatch");
        }
        return new BTreeIndex(index.indexId(), binding.rootPageId(), binding.rootLevel(),
                mapper.indexKey(table, index), mapper.tableSchema(table, index.clustered()), index.unique(),
                binding.leafSegment(), binding.nonLeafSegment());
    }
}
