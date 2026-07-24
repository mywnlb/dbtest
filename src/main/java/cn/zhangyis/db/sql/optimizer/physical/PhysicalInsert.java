package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.InsertBatch;
import cn.zhangyis.db.sql.type.InsertValueSource;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 向聚簇记录写入一行的物理计划。
 *
 * @param table exact DD table version
 * @param values 按 table column ordinal 排列的完整 typed row
 */
public record PhysicalInsert(TableDefinition table, InsertBatch batch)
        implements PhysicalPlan {
    /**
     * 校验并复制一行完整 typed values。
     *
     * @throws DatabaseValidationException table/values 缺失、行宽不符或含 Java {@code null} 时抛出
     */
    public PhysicalInsert {
        if (table == null || batch == null
                || batch.width() != table.columns().size()) {
            throw new DatabaseValidationException(
                    "physical INSERT rows must match exact table width");
        }
    }

    /**
     * 保留 v1 单行常量构造入口。
     *
     * @param table exact table
     * @param values 完整 typed row
     */
    public PhysicalInsert(TableDefinition table, List<SqlValue> values) {
        this(table, InsertBatch.constants(values));
    }

    /**
     * 返回首行常量值以兼容 v1 调用点；新批量/自增执行必须使用 {@link #batch()}。
     *
     * @return 第一行 typed constants
     */
    public List<SqlValue> values() {
        return batch.rows().getFirst().stream().map(source -> {
            if (source instanceof InsertValueSource.Constant constant) {
                return constant.value();
            }
            throw new DatabaseValidationException(
                    "AUTO_INCREMENT physical INSERT requires batch execution");
        }).toList();
    }
}
