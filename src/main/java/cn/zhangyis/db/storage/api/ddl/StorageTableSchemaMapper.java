package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.CharsetId;
import cn.zhangyis.db.storage.record.schema.CollationId;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.StorageKind;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.schema.TypeId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 把稳定 storage API DTO 映射为 Record/B+Tree 内部 schema；唯一的跨边界类型适配点。 */
final class StorageTableSchemaMapper {

    /**
     * DD columnId 只作为稳定身份；Record 层要求 ColumnId==ordinal，因此先建立显式映射，索引键不得直接强转 id。
     */
    TableSchema tableSchema(StorageTableDefinition definition, boolean clustered) {
        List<ColumnDef> columns = definition.columns().stream()
                .map(column -> new ColumnDef(new ColumnId(column.ordinal()), column.name(),
                        columnType(column.type()), column.ordinal()))
                .toList();
        return new TableSchema(definition.schemaVersion(), columns, clustered);
    }

    /** 把 index key 中的 DD columnId 解析为本表物理 ordinal。 */
    IndexKeyDef indexKey(StorageTableDefinition table, StorageIndexDefinition index) {
        Map<Long, Integer> ordinals = new HashMap<>();
        for (StorageColumnDefinition column : table.columns()) {
            ordinals.put(column.columnId(), column.ordinal());
        }
        List<KeyPartDef> parts = index.keyParts().stream().map(part -> {
            Integer ordinal = ordinals.get(part.columnId());
            if (ordinal == null) {
                throw new DatabaseValidationException("index key references missing storage column: "
                        + part.columnId());
            }
            return new KeyPartDef(new ColumnId(ordinal),
                    part.order() == StorageIndexOrder.ASC ? KeyOrder.ASC : KeyOrder.DESC,
                    part.prefixBytes());
        }).toList();
        return new IndexKeyDef(index.indexId(), parts);
    }

    /** stable type id/charset/collation 显式映射到物理类型；未知编号 fail-closed，不回退默认。 */
    private static ColumnType columnType(StorageColumnType type) {
        TypeId typeId = TypeId.valueOf(type.typeId().name());
        StorageKind storageKind = switch (typeId) {
            case VARCHAR, VARBINARY -> StorageKind.VARIABLE;
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
                    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> StorageKind.OVERFLOW_CAPABLE;
            default -> StorageKind.FIXED;
        };
        CharsetId charset = type.charsetId() == 0 ? CharsetId.UTF8 : CharsetId.fromStableId(type.charsetId());
        CollationId collation = type.collationId() == 0 ? CollationId.BINARY
                : CollationId.fromStableId(type.collationId());
        return new ColumnType(typeId, type.nullable(), type.length(), type.scale(), type.unsigned(),
                charset, collation,
                storageKind, type.symbols());
    }
}
