package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.domain.PageNo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** CREATE TABLE 的逻辑命令；构造阶段完成名称唯一性和聚簇索引数量校验。
 *
 * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param initialSizeInPages 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
 * @param columns 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 * @param indexes 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 */
public record CreateTableCommand(QualifiedTableName name, PageNo initialSizeInPages,
                                 List<CreateColumnSpec> columns, List<CreateIndexSpec> indexes) {
    public CreateTableCommand {
        if (name == null || initialSizeInPages == null || columns == null || columns.isEmpty()
                || indexes == null || indexes.isEmpty()) {
            throw new DatabaseValidationException("create table name/size/columns/indexes required");
        }
        columns = List.copyOf(columns);
        indexes = List.copyOf(indexes);
        Set<Object> columnNames = new HashSet<>();
        for (CreateColumnSpec column : columns) {
            if (!columnNames.add(column.name())) {
                throw new DatabaseValidationException("duplicate CREATE column name: " + column.name());
            }
        }
        Set<Object> indexNames = new HashSet<>();
        for (CreateIndexSpec index : indexes) {
            if (!indexNames.add(index.name())) {
                throw new DatabaseValidationException("duplicate CREATE index name: " + index.name());
            }
            for (CreateIndexKeyPartSpec part : index.keyParts()) {
                if (!columnNames.contains(part.columnName())) {
                    throw new DatabaseValidationException("CREATE index references missing column: "
                            + part.columnName());
                }
            }
        }
        if (indexes.stream().filter(CreateIndexSpec::clustered).count() != 1) {
            throw new DatabaseValidationException("CREATE table requires exactly one clustered index");
        }
    }
}
