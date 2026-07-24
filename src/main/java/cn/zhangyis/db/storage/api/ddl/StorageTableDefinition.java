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
 *
 * @param tableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
 * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
 * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
 * @param schemaVersion 参与 {@code 构造} 的单调版本值 {@code schemaVersion}；必须非负，回退或与权威快照冲突时拒绝
 * @param initialSizeInPages 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
 * @param columns 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 * @param indexes 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 * @param autoIncrement 是否初始化页 0 自增格式为 active；列位置/类型已由 DD 验证
 */
public record StorageTableDefinition(long tableId, SpaceId spaceId, Path path, long schemaVersion,
                                     PageNo initialSizeInPages, List<StorageColumnDefinition> columns,
                                     List<StorageIndexDefinition> indexes,
                                     boolean autoIncrement) {
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

    /**
     * 保留无自增语义的 v1 构造入口。
     */
    public StorageTableDefinition(
            long tableId, SpaceId spaceId, Path path, long schemaVersion,
            PageNo initialSizeInPages, List<StorageColumnDefinition> columns,
            List<StorageIndexDefinition> indexes) {
        this(tableId, spaceId, path, schemaVersion, initialSizeInPages,
                columns, indexes, false);
    }
}
