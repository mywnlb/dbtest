package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 已按 exact DD column ordinal 完成类型转换的单行 INSERT 语义。
 *
 * @param table statement metadata lease 固定的 exact table version；后续 logical/physical plan 必须沿用该版本
 * @param values 按 table column ordinal 排列的完整逻辑行；SQL NULL 使用值对象表达，不允许 Java {@code null}
 */
public record BoundInsert(TableDefinition table, List<SqlValue> values)
        implements BoundRelationalStatement {

    /**
     * 冻结完整逻辑行，禁止 Binder 把缺列或可变集合发布给 Optimizer。
     *
     * @throws DatabaseValidationException table 缺失、行宽不一致或存在 Java {@code null} 时抛出
     */
    public BoundInsert {
        if (table == null || values == null || values.size() != table.columns().size()
                || values.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "bound INSERT must contain one value per exact table column");
        }
        values = List.copyOf(values);
    }
}
