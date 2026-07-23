package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 向聚簇记录写入一行的物理计划。
 *
 * @param table exact DD table version
 * @param values 按 table column ordinal 排列的完整 typed row
 */
public record PhysicalInsert(TableDefinition table, List<SqlValue> values)
        implements PhysicalPlan {
    /**
     * 校验并复制一行完整 typed values。
     *
     * @throws DatabaseValidationException table/values 缺失、行宽不符或含 Java {@code null} 时抛出
     */
    public PhysicalInsert {
        if (table == null || values == null || values.size() != table.columns().size()
                || values.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "physical INSERT must contain one value per exact table column");
        }
        values = List.copyOf(values);
    }
}
