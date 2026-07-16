package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 物理建表请求。它是 DD→storage.api 的稳定 DTO，不携带 DD cache/MDL 对象，也不暴露 BufferFrame/BTreeIndex。
 */
public record StorageTableDefinition(long tableId, SpaceId spaceId, Path path, long schemaVersion,
                                     PageNo initialSizeInPages, List<StorageColumnDefinition> columns,
                                     List<StorageIndexDefinition> indexes) {
    public StorageTableDefinition {
        if (tableId <= 0 || spaceId == null || path == null || schemaVersion <= 0 || initialSizeInPages == null
                || columns == null || columns.isEmpty() || indexes == null || indexes.isEmpty()) {
            throw new DatabaseValidationException("invalid storage table definition");
        }
        path = path.toAbsolutePath().normalize();
        columns = List.copyOf(columns);
        indexes = List.copyOf(indexes);
        Set<Long> ids = new HashSet<>();
        for (int ordinal = 0; ordinal < columns.size(); ordinal++) {
            StorageColumnDefinition column = columns.get(ordinal);
            if (column.ordinal() != ordinal || !ids.add(column.columnId())) {
                throw new DatabaseValidationException("storage columns require continuous ordinal and unique id");
            }
        }
        long clustered = indexes.stream().filter(StorageIndexDefinition::clustered).count();
        if (clustered != 1) {
            throw new DatabaseValidationException("storage table requires exactly one clustered index");
        }
        Set<Long> indexIds = new HashSet<>();
        Set<String> indexNames = new HashSet<>();
        for (StorageIndexDefinition index : indexes) {
            if (!indexIds.add(index.indexId())
                    || !indexNames.add(index.name().toLowerCase(Locale.ROOT))) {
                throw new DatabaseValidationException("duplicate storage index id/name: "
                        + index.indexId() + "/" + index.name());
            }
            for (StorageIndexKeyPart part : index.keyParts()) {
                if (!ids.contains(part.columnId())) {
                    throw new DatabaseValidationException("storage index references missing column: " + part.columnId());
                }
            }
        }
    }
}
