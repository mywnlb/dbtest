package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.engine.adapter.exception.DictionaryStorageMappingException;
import cn.zhangyis.db.storage.api.ddl.BTreeIndexMetadataFactory;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.StorageColumnDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageColumnType;
import cn.zhangyis.db.storage.api.ddl.StorageColumnTypeId;
import cn.zhangyis.db.storage.api.ddl.StorageIndexDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageIndexKeyPart;
import cn.zhangyis.db.storage.api.ddl.StorageIndexOrder;
import cn.zhangyis.db.storage.api.ddl.StorageTableDefinition;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.btree.BTreeIndex;

import java.util.List;

/**
 * DD → storage metadata 的无状态 Adapter。输入必须是调用方已通过 lease 固定的完整 {@link TableDefinition}；本类没有
 * repository 依赖，因此不可能把旧 lease 偷换为最新 catalog 版本。recovery resolver 可以先自行按 identity 查表，再复用
 * 同一映射规则。
 */
public final class DictionaryStorageMetadataMapper {

    /** runtime schema DTO 只参与映射，initialSize 不参与已有表打开；使用最小合法值避免伪造 catalog 文件大小。 */
    private static final PageNo RUNTIME_MAPPING_INITIAL_SIZE = PageNo.of(1);

    /** storage API 内唯一的 BTree descriptor 工厂。 */
    private final BTreeIndexMetadataFactory factory = new BTreeIndexMetadataFactory();

    /**
     * 映射一个精确 DD 版本。ACTIVE 与 recovery 可见的 DROP_PENDING 可映射；DROPPED 或无 binding 必须 fail-closed。
     * 所有逻辑/物理 index 先完整配对，再返回结果，避免半映射对象逃逸。
     */
    public MappedTableStorage map(TableDefinition table) {
        if (table == null) {
            throw new DictionaryStorageMappingException("dictionary table to map must not be null");
        }
        if (table.state() == TableState.DROPPED) {
            throw new DictionaryStorageMappingException("cannot map dropped dictionary table: "
                    + table.id().value());
        }
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryStorageMappingException("dictionary table has no storage binding: "
                        + table.id().value()));
        try {
            StorageTableDefinition storageTable = new StorageTableDefinition(table.id().value(), binding.spaceId(),
                    binding.path(), table.version().value(), RUNTIME_MAPPING_INITIAL_SIZE,
                    columns(table), indexes(table));
            List<BTreeIndex> mappedIndexes = table.indexes().stream()
                    .map(index -> factory.create(storageTable, storageIndex(storageTable, index.id().value()),
                            indexBinding(binding, index.id().value())))
                    .toList();
            return new MappedTableStorage(table, storageTable, binding, binding.lobSegment(), mappedIndexes);
        } catch (DictionaryStorageMappingException mappingFailure) {
            throw mappingFailure;
        } catch (DatabaseRuntimeException invalidMetadata) {
            throw new DictionaryStorageMappingException("invalid storage metadata for table "
                    + table.id().value() + " version " + table.version().value(), invalidMetadata);
        }
    }

    /** DD columnId 保持稳定身份，physical ordinal 由 column.ordinal 显式传入 storage DTO。 */
    private static List<StorageColumnDefinition> columns(TableDefinition table) {
        return table.columns().stream().map(column -> new StorageColumnDefinition(column.columnId(),
                column.name().displayName(), column.ordinal(), storageType(column))).toList();
    }

    /** 显式复制全部类型属性；未知 stable enum 不使用默认类型兜底。 */
    private static StorageColumnType storageType(ColumnDefinition column) {
        var type = column.type();
        return new StorageColumnType(StorageColumnTypeId.valueOf(type.typeId().name()), type.nullable(),
                type.length(), type.scale(), type.unsigned(), type.charsetId(), type.collationId(), type.symbols());
    }

    /** index key 的 DD columnId/order/prefix 原样进入稳定 storage DTO。 */
    private static List<StorageIndexDefinition> indexes(TableDefinition table) {
        return table.indexes().stream().map(index -> new StorageIndexDefinition(index.id().value(),
                index.name().displayName(), index.unique(), index.clustered(), index.keyParts().stream()
                .map(part -> new StorageIndexKeyPart(part.columnId(),
                        part.order() == cn.zhangyis.db.dd.domain.IndexOrder.ASC
                                ? StorageIndexOrder.ASC : StorageIndexOrder.DESC,
                        part.prefixBytes())).toList())).toList();
    }

    /** 从本次刚派生的 DTO 定位逻辑 index，禁止重新读取 DD。 */
    private static StorageIndexDefinition storageIndex(StorageTableDefinition table, long indexId) {
        return table.indexes().stream().filter(index -> index.indexId() == indexId).findFirst().orElseThrow(() ->
                new DictionaryStorageMappingException("mapped storage index definition missing: " + indexId));
    }

    /** 从同一 DD aggregate 携带的 binding 定位物理 index，禁止使用其它表或其它版本的 root。 */
    private static IndexStorageBinding indexBinding(TableStorageBinding table, long indexId) {
        return table.indexes().stream().filter(index -> index.indexId() == indexId).findFirst().orElseThrow(() ->
                new DictionaryStorageMappingException("mapped index storage binding missing: " + indexId));
    }
}
