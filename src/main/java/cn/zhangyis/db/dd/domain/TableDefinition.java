package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Optional;

/**
 * Data Dictionary table 聚合根。构造阶段一次性验证列布局、唯一名称和聚簇主键引用，保证进入 cache/catalog
 * 后对象始终可安全映射为 Record/B+Tree schema，不依赖调用方再次补校验。
 *
 * @param id 参与 {@code 构造} 的稳定领域标识 {@code TableId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param schemaId 参与 {@code 构造} 的稳定领域标识 {@code SchemaId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
 * @param columns 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 * @param indexes 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 * @param storageBinding 可选的 {@code storageBinding}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
 */
public record TableDefinition(TableId id, SchemaId schemaId, ObjectName name, DictionaryVersion version,
                              TableState state, List<ColumnDefinition> columns,
                              List<IndexDefinition> indexes, Optional<TableStorageBinding> storageBinding) {
    public TableDefinition {
        if (id == null || schemaId == null || name == null || version == null || state == null
                || columns == null || indexes == null || storageBinding == null
                || columns.isEmpty() || indexes.isEmpty()) {
            throw new DatabaseValidationException("table definition fields/columns/indexes must not be null or empty");
        }
        columns = List.copyOf(columns);
        indexes = List.copyOf(indexes);
        Set<Long> columnIds = new HashSet<>();
        Set<ObjectName> columnNames = new HashSet<>();
        for (int ordinal = 0; ordinal < columns.size(); ordinal++) {
            ColumnDefinition column = columns.get(ordinal);
            if (column.ordinal() != ordinal || !columnIds.add(column.columnId()) || !columnNames.add(column.name())) {
                throw new DatabaseValidationException("table columns must have continuous ordinals and unique id/name");
            }
        }
        long clusteredCount = indexes.stream().filter(IndexDefinition::clustered).count();
        if (clusteredCount != 1) {
            throw new DatabaseValidationException("table must have exactly one clustered primary index");
        }
        Set<IndexId> indexIds = new HashSet<>();
        Set<ObjectName> indexNames = new HashSet<>();
        for (IndexDefinition index : indexes) {
            if (!indexIds.add(index.id()) || !indexNames.add(index.name())) {
                throw new DatabaseValidationException("duplicate index id/name: "
                        + index.id().value() + "/" + index.name());
            }
            for (IndexKeyPart part : index.keyParts()) {
                if (!columnIds.contains(part.columnId())) {
                    throw new DatabaseValidationException("index references missing column id: " + part.columnId());
                }
            }
        }
        if (storageBinding.isPresent()) {
            TableStorageBinding binding = storageBinding.orElseThrow(() ->
                    new DatabaseValidationException("present table storage binding unexpectedly missing"));
            if (binding.tableId() != id.value()) {
                throw new DatabaseValidationException("table/storage binding id mismatch");
            }
            Set<Long> logicalIndexes = indexes.stream().map(index -> index.id().value())
                    .collect(java.util.stream.Collectors.toSet());
            Set<Long> physicalIndexes = binding.indexes().stream().map(index -> index.indexId())
                    .collect(java.util.stream.Collectors.toSet());
            if (!logicalIndexes.equals(physicalIndexes)) {
                throw new DatabaseValidationException("table/storage binding index set mismatch");
            }
        }
    }

    /** 兼容纯逻辑定义的构造器；只有物理 CREATE 完成后才由 DDL 协调器填入 binding。 */
    public TableDefinition(TableId id, SchemaId schemaId, ObjectName name, DictionaryVersion version,
                           TableState state, List<ColumnDefinition> columns, List<IndexDefinition> indexes) {
        this(id, schemaId, name, version, state, columns, indexes, Optional.empty());
    }

    /** 返回唯一聚簇主键；构造器已经保证恰好一个。 */
    public IndexDefinition primaryIndex() {
        return indexes.stream().filter(IndexDefinition::clustered).findFirst().orElseThrow(() ->
                new DatabaseValidationException("table clustered index invariant was lost"));
    }
}
