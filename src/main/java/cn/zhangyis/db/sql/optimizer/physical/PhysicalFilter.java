package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;

/**
 * 在访问候选的完整逻辑行上执行最终 SQL 三值判断的物理过滤算子。
 *
 * @param input 同一 exact table version 的访问输入
 * @param predicates 规则固定点后的完整 canonical residual
 */
public record PhysicalFilter(PhysicalAccess input, PredicateSet predicates)
        implements PhysicalOperator {

    /**
     * 校验输入与 residual 属于同一 metadata version。
     *
     * @throws DatabaseValidationException 输入或 residual 不完整、未规范时抛出
     */
    public PhysicalFilter {
        if (input == null || predicates == null) {
            throw new DatabaseValidationException("physical filter fields must not be null");
        }
        PhysicalPlanValidation.validateCanonicalResidual(input.table(), predicates);
    }

    @Override
    public TableDefinition table() {
        return input.table();
    }
}
