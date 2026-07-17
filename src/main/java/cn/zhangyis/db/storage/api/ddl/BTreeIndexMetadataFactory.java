package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.record.schema.SecondaryIndexLayout;

import java.util.Comparator;
import java.util.List;

/**
 * storage API 内把稳定 DDL DTO/binding 组装成 B+Tree 运行期快照的唯一工厂。工厂只映射不可变 metadata，
 * 不打开 root 页；binding 的 root level 因而只是 bootstrap hint，结构写前仍须读取页头刷新。
 */
public final class BTreeIndexMetadataFactory {

    /** DDL 列/索引 DTO 到 record schema/key definition 的纯映射器。 */
    private final StorageTableSchemaMapper mapper = new StorageTableSchemaMapper();

    /**
     * 从单个索引定义及其物理 binding 构造运行期 descriptor。
     *
     * @param table   提供表 id、space id、schema version 和完整列定义的稳定 DDL DTO。
     * @param index   属于该表的逻辑索引定义；clustered 属性决定 leaf 是否携带隐藏列。
     * @param binding 与 {@code index.indexId} 对应的 root/leaf-segment/non-leaf-segment 物理绑定。
     * @return identity、schema 与 key definition 已冻结的 B+Tree descriptor；完整 physical key 始终唯一。
     * @throws DatabaseValidationException 输入缺失、index id 不一致或 root 不属于目标 tablespace 时抛出。
     */
    public BTreeIndex create(StorageTableDefinition table, StorageIndexDefinition index,
                             IndexStorageBinding binding) {
        if (table == null || index == null || binding == null || index.indexId() != binding.indexId()
                || !binding.rootPageId().spaceId().equals(table.spaceId())) {
            throw new DatabaseValidationException("BTree metadata factory input identity mismatch");
        }
        return new BTreeIndex(index.indexId(), binding.rootPageId(), binding.rootLevel(),
                mapper.indexKey(table, index), mapper.tableSchema(table, index.clustered()), true,
                binding.leafSegment(), binding.nonLeafSegment());
    }

    /**
     * 把同一 table definition/binding 映射为完整运行期索引聚合。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>定位唯一聚簇定义和 binding，构造携带完整表 schema 的聚簇 descriptor。</li>
     *     <li>对每个二级定义构造紧凑 layout，并用 logical key + 完整主键生成物理唯一 descriptor。</li>
     *     <li>按 index id 排序后发布不可变 table snapshot；本工厂不读取页，binding level 仍只是 bootstrap hint。</li>
     * </ol>
     *
     * @param table   目标表的稳定 DDL DTO；聚簇/二级定义与完整列 schema 均从这里读取。
     * @param binding 与该表 id/space 对应的全部索引物理 binding。
     * @return 一个聚簇索引和按 index id 排序的全部二级 exact-version metadata 聚合。
     * @throws DatabaseValidationException 输入 identity 错配、缺少聚簇定义/任一物理 binding，或二级 layout
     *                                     无法由当前 schema/key definition 构造时抛出。
     */
    public TableIndexMetadata createTable(StorageTableDefinition table, TableStorageBinding binding) {
        // 1. 聚簇 descriptor 提供二级布局所需的完整表 schema 与主键定义。
        if (table == null || binding == null || table.tableId() != binding.tableId()
                || !table.spaceId().equals(binding.spaceId())) {
            throw new DatabaseValidationException("table index metadata factory identity mismatch");
        }
        StorageIndexDefinition primaryDefinition = table.indexes().stream()
                .filter(StorageIndexDefinition::clustered).findFirst().orElseThrow(() ->
                        new DatabaseValidationException("storage table has no clustered index"));
        BTreeIndex clustered = create(table, primaryDefinition,
                requireBinding(binding, primaryDefinition.indexId()));

        // 2. 二级 descriptor 的 schema/keyDef 来自紧凑 layout，physicalUnique 恒为 true。
        List<SecondaryIndexMetadata> secondaries = table.indexes().stream()
                .filter(index -> !index.clustered())
                .map(index -> {
                    SecondaryIndexLayout layout = SecondaryIndexLayout.create(clustered.schema(),
                            mapper.indexKey(table, index), clustered.keyDef());
                    IndexStorageBinding physical = requireBinding(binding, index.indexId());
                    BTreeIndex btree = new BTreeIndex(index.indexId(), physical.rootPageId(), physical.rootLevel(),
                            layout.physicalKeyDef(), layout.entrySchema(), true,
                            physical.leafSegment(), physical.nonLeafSegment());
                    return new SecondaryIndexMetadata(btree, layout, index.unique());
                })
                .sorted(Comparator.comparingLong(metadata -> metadata.index().indexId()))
                .toList();

        // 3. 只发布来自同一输入 aggregate 的快照，不回查 DD 或打开物理 root。
        return new TableIndexMetadata(table.tableId(), table.schemaVersion(), clustered, secondaries);
    }

    /**
     * 在表级物理绑定中按稳定 index id 定位单索引 binding。
     *
     * @param binding 当前 exact table version 的物理绑定聚合。
     * @param indexId DDL 索引定义声明的稳定 id。
     * @return root/segment 均属于该表 tablespace 的匹配 binding。
     * @throws DatabaseValidationException 聚合中不存在该索引时抛出；禁止回退到其它 root 或索引。
     */
    private static IndexStorageBinding requireBinding(TableStorageBinding binding, long indexId) {
        return binding.indexes().stream().filter(candidate -> candidate.indexId() == indexId)
                .findFirst().orElseThrow(() -> new DatabaseValidationException(
                        "table binding has no index " + indexId));
    }
}
