package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * INSERT 的单行逻辑 values 输入。
 *
 * @param table exact table version
 * @param values 按 column ordinal 排列的完整 typed row
 */
public record LogicalValues(TableDefinition table, List<SqlValue> values)
        implements RelNode {
    /**
     * 校验并复制一行完整 typed values。
     *
     * @throws DatabaseValidationException table/values 缺失、行宽不符或包含 Java {@code null} 时抛出
     */
    public LogicalValues {
        if (table == null || values == null || values.size() != table.columns().size()
                || values.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("logical values row does not match table width");
        }
        values = List.copyOf(values);
    }
}
