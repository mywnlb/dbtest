package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.InsertBatch;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * INSERT 的单行逻辑 values 输入。
 *
 * @param table exact table version
 * @param values 按 column ordinal 排列的完整 typed row
 */
public record LogicalValues(TableDefinition table, InsertBatch batch)
        implements RelNode {
    /**
     * 校验并复制一行完整 typed values。
     *
     * @throws DatabaseValidationException table/values 缺失、行宽不符或包含 Java {@code null} 时抛出
     */
    public LogicalValues {
        if (table == null || batch == null
                || batch.width() != table.columns().size()) {
            throw new DatabaseValidationException("logical values row does not match table width");
        }
    }

    /**
     * 保留 v1 单行常量构造入口。
     *
     * @param table exact table
     * @param values 完整 typed row
     */
    public LogicalValues(TableDefinition table, List<SqlValue> values) {
        this(table, InsertBatch.constants(values));
    }
}
