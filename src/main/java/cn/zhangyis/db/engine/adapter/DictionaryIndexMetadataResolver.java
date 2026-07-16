package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.domain.PageNo;
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
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.btree.BTreeIndex;

import java.util.List;

/** committed DD table/index 定义与物理 binding 的 rollback/purge adapter。无全局默认索引或名称猜测。 */
public final class DictionaryIndexMetadataResolver implements IndexMetadataResolver {

    private final PersistentDictionaryRepository repository;
    private final BTreeIndexMetadataFactory factory = new BTreeIndexMetadataFactory();

    public DictionaryIndexMetadataResolver(PersistentDictionaryRepository repository) {
        if (repository == null) {
            throw new DatabaseValidationException("dictionary index resolver repository must not be null");
        }
        this.repository = repository;
    }

    /** tableId/indexId 双身份必须命中同一个非 DROPPED 聚簇/二级索引定义和 binding。 */
    @Override
    public BTreeIndex resolve(long tableId, long indexId) {
        TableDefinition table = repository.findTableForRecovery(TableId.of(tableId)).orElseThrow(() ->
                new DictionaryObjectNotFoundException("undo table metadata not found: " + tableId));
        if (table.state() == TableState.DROPPED) {
            throw new DictionaryObjectNotFoundException("undo references dropped table: " + tableId);
        }
        IndexDefinition logical = table.indexes().stream()
                .filter(index -> index.id().value() == indexId).findFirst().orElseThrow(() ->
                        new DictionaryObjectNotFoundException("undo index metadata not found: table="
                                + tableId + " index=" + indexId));
        TableStorageBinding tableBinding = table.storageBinding().orElseThrow(() ->
                new DictionaryObjectNotFoundException("undo table has no storage binding: " + tableId));
        IndexStorageBinding binding = tableBinding.indexes().stream()
                .filter(index -> index.indexId() == indexId).findFirst().orElseThrow(() ->
                        new DictionaryObjectNotFoundException("undo index has no storage binding: " + indexId));
        StorageTableDefinition storageTable = new StorageTableDefinition(table.id().value(), tableBinding.spaceId(),
                tableBinding.path(), table.version().value(), PageNo.of(1), columns(table), indexes(table));
        StorageIndexDefinition storageIndex = storageTable.indexes().stream()
                .filter(index -> index.indexId() == indexId).findFirst().orElseThrow(() ->
                        new DictionaryObjectNotFoundException("mapped storage index not found: " + indexId));
        return factory.create(storageTable, storageIndex, binding);
    }

    private static List<StorageColumnDefinition> columns(TableDefinition table) {
        return table.columns().stream().map(column -> new StorageColumnDefinition(column.columnId(),
                column.name().displayName(), column.ordinal(), storageType(column))).toList();
    }

    private static StorageColumnType storageType(ColumnDefinition column) {
        var type = column.type();
        return new StorageColumnType(StorageColumnTypeId.valueOf(type.typeId().name()), type.nullable(),
                type.length(), type.scale(), type.unsigned(), type.charsetId(), type.collationId(), type.symbols());
    }

    private static List<StorageIndexDefinition> indexes(TableDefinition table) {
        return table.indexes().stream().map(index -> new StorageIndexDefinition(index.id().value(),
                index.name().displayName(), index.unique(), index.clustered(), index.keyParts().stream()
                .map(part -> new StorageIndexKeyPart(part.columnId(),
                        part.order() == cn.zhangyis.db.dd.domain.IndexOrder.ASC
                                ? StorageIndexOrder.ASC : StorageIndexOrder.DESC,
                        part.prefixBytes())).toList())).toList();
    }
}
