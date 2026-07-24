package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.InsertBatch;
import cn.zhangyis.db.sql.type.InsertValueSource;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 已按 exact DD column ordinal 完成类型转换和缺省补齐的批量 INSERT 语义。
 *
 * @param table statement metadata lease 固定的 exact table version；后续 logical/physical plan 必须沿用该版本
 * @param batch 按 table column ordinal 排列的完整逻辑行批次
 */
public record BoundInsert(TableDefinition table, InsertBatch batch)
        implements BoundRelationalStatement {

    /**
     * 冻结完整逻辑行，禁止 Binder 把缺列或可变集合发布给 Optimizer。
     *
     * @throws DatabaseValidationException table 缺失、行宽不一致或存在 Java {@code null} 时抛出
     */
    public BoundInsert {
        if (table == null || batch == null
                || batch.width() != table.columns().size()) {
            throw new DatabaseValidationException(
                    "bound INSERT rows must match exact table width");
        }
    }

    /**
     * 保留 v1 单行构造形状。
     *
     * @param table exact DD table
     * @param values 完整 typed row
     */
    public BoundInsert(TableDefinition table, List<SqlValue> values) {
        this(table, InsertBatch.constants(values));
    }

    /**
     * 返回首行常量值以兼容 v1 只读调用点；含自增请求时调用者必须改用 {@link #batch()}。
     *
     * @return 第一行 typed constants
     */
    public List<SqlValue> values() {
        return batch.rows().getFirst().stream().map(source -> {
            if (source instanceof InsertValueSource.Constant constant) {
                return constant.value();
            }
            throw new DatabaseValidationException(
                    "AUTO_INCREMENT INSERT must be consumed through batch sources");
        }).toList();
    }
}
