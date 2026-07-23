package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;

/**
 * 未选择访问路径的单表扫描。
 *
 * @param table exact table version
 * @param readMode 一致性读或 current locking read；该语义不能被规则转换
 */
public record LogicalTableScan(TableDefinition table, SelectLockMode readMode)
        implements RelNode {
    /**
     * 冻结 exact table version 与不可改写的读意图。
     *
     * @throws DatabaseValidationException table 或 read mode 缺失时抛出
     */
    public LogicalTableScan {
        if (table == null || readMode == null) {
            throw new DatabaseValidationException("logical table scan fields must not be null");
        }
    }
}
