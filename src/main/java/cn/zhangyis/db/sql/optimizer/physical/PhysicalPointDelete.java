package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 完整聚簇主键定位的点 DELETE。
 *
 * @param table exact DD table version
 * @param primaryKeyValues 按聚簇 index key-part 顺序排列的完整定位值
 */
public record PhysicalPointDelete(TableDefinition table, List<SqlValue> primaryKeyValues)
        implements PhysicalPlan {
    /**
     * 校验并冻结完整聚簇记录身份。
     *
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException
     *         主键不完整、含 SQL NULL、使用 prefix/LOB key 或 table 缺失时抛出
     */
    public PhysicalPointDelete {
        PhysicalPlanValidation.validatePrimaryKey(table, primaryKeyValues, "DELETE");
        primaryKeyValues = List.copyOf(primaryKeyValues);
    }
}
