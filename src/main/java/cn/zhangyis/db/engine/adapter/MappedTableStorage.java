package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.engine.adapter.exception.DictionaryStorageMappingException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.StorageTableDefinition;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 一个调用方固定 DD 版本对应的不可变 storage metadata 快照。该对象不查询 repository；table、storage DTO、binding
 * 和全部 B+Tree descriptor 必须来自同一个 {@link TableDefinition}，避免执行阶段按 tableId 偷换到较新 root。
 */
public final class MappedTableStorage {

    /** 调用方持有 lease 时看到的精确 DD aggregate；版本和生命周期身份的权威来源。 */
    private final TableDefinition table;

    /** 由同一 aggregate 派生的 storage schema DTO；schemaVersion 必须等于 binding 的物理行格式版本。 */
    private final StorageTableDefinition storageTable;

    /** 同一 aggregate 持久化的物理表绑定；后续 LOB 扩展也只能从这里取得 segment。 */
    private final TableStorageBinding binding;

    /** 精确 DD 版本的表级 LOB segment；empty 表示旧表只允许 inline LOB，不能在普通 INSERT 中补建。 */
    private final Optional<SegmentRef> lobSegment;

    /** 按逻辑 index 顺序保存的不可变运行期 descriptor。 */
    private final List<BTreeIndex> indexes;

    /** 同一 DD aggregate 派生的表级索引布局，供多索引 DML/MVCC/rollback 使用。 */
    private final TableIndexMetadata tableIndexes;

    /** indexId 到 descriptor 的只读定位表；构造后不再变化。 */
    private final Map<Long, BTreeIndex> indexesById;

    /**
     * 组装精确版本快照；这里只校验跨对象 identity，字段/索引物理细节由各自构造器和 mapper 保证。
     *
     * @param table        调用方通过 catalog lease 固定的 DD table aggregate。
     * @param storageTable 从同一 aggregate 映射的 storage schema DTO。
     * @param binding      与该表 id、版本和 space 对应的持久物理绑定。
     * @param lobSegment   binding 中属于该 exact table version 的可选 LOB segment。
     * @param tableIndexes 从同一 storage DTO/binding 构造的聚簇与全部二级运行期元数据。
     * @throws DatabaseValidationException 字段缺失、table/version/space/LOB identity 不一致，或运行期索引数量/id
     *                                     无法与 DD aggregate 一一对应时抛出。
     */
    MappedTableStorage(TableDefinition table, StorageTableDefinition storageTable,
                       TableStorageBinding binding, Optional<SegmentRef> lobSegment,
                       TableIndexMetadata tableIndexes) {
        if (table == null || storageTable == null || binding == null || lobSegment == null
                || tableIndexes == null) {
            throw new DatabaseValidationException("mapped table storage fields/indexes must not be null or empty");
        }
        if (table.id().value() != storageTable.tableId() || table.id().value() != binding.tableId()
                || binding.rowFormatVersion() != storageTable.schemaVersion()
                || !storageTable.spaceId().equals(binding.spaceId())
                || !binding.lobSegment().equals(lobSegment)) {
            throw new DatabaseValidationException("mapped table storage identity/version mismatch");
        }
        this.table = table;
        this.storageTable = storageTable;
        this.binding = binding;
        this.lobSegment = lobSegment;
        this.tableIndexes = tableIndexes;
        this.indexes = tableIndexes.allIndexes();
        Map<Long, BTreeIndex> mapped = new LinkedHashMap<>();
        for (BTreeIndex index : this.indexes) {
            if (mapped.put(index.indexId(), index) != null) {
                throw new DatabaseValidationException("duplicate mapped BTree index id: " + index.indexId());
            }
        }
        if (mapped.size() != table.indexes().size()) {
            throw new DatabaseValidationException("mapped BTree index count differs from DD table");
        }
        this.indexesById = Map.copyOf(mapped);
    }

    /** 返回调用方固定的原始 DD aggregate，不执行 repository lookup。 */
    public TableDefinition table() {
        return table;
    }

    /** 返回从该版本派生的 storage schema DTO。 */
    public StorageTableDefinition storageTable() {
        return storageTable;
    }

    /** 返回该版本携带的权威物理 binding。 */
    public TableStorageBinding binding() {
        return binding;
    }

    /** 返回该精确 catalog 版本的外部化能力；empty 时 inline LOB 仍可用。 */
    public Optional<SegmentRef> lobSegment() {
        return lobSegment;
    }

    /** 返回全部不可变 B+Tree descriptor，顺序与 DD indexes 相同。 */
    public List<BTreeIndex> indexes() {
        return indexes;
    }

    /**
     * 返回精确 DD 版本的表级聚簇/二级运行期 metadata。
     *
     * @return 与 {@link #table()}、{@link #storageTable()} 和 {@link #binding()} 同源的不可变索引聚合；
     *         多索引 DML/rollback/MVCC 不得按 table id 回查并替换它。
     */
    public TableIndexMetadata tableIndexes() {
        return tableIndexes;
    }

    /**
     * 按稳定 indexId 返回 descriptor；未知 id 表示调用方使用了错误的 bound index，禁止回退聚簇索引。
     *
     * @param indexId 参与 {@code index} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code index} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DictionaryStorageMappingException 字典定义无法完整映射为当前存储 schema 或索引绑定时抛出；调用方不得发布半映射对象，应刷新元数据或回滚 DDL
     */
    public BTreeIndex index(long indexId) {
        BTreeIndex index = indexesById.get(indexId);
        if (index == null) {
            throw new DictionaryStorageMappingException("mapped table " + table.id().value()
                    + " has no index " + indexId);
        }
        return index;
    }

    /** 返回 DD 构造器保证唯一的聚簇 B+Tree descriptor。 */
    public BTreeIndex clusteredIndex() {
        return tableIndexes.clusteredIndex();
    }
}
