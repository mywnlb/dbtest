package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.expression.BoundExpressionValidation;

/**
 * 保存最终 SQL conjunction 的逻辑过滤节点。
 *
 * @param input 同一 exact table version 的输入关系
 * @param predicates typed residual；condition 是唯一权威状态，访问路径不能删除
 */
public record LogicalFilter(RelNode input, PredicateSet predicates)
        implements RelNode {
    /**
     * 校验 predicate 容器及其相对 exact table version 的 ordinal，并复制 residual。
     *
     * @throws DatabaseValidationException 输入、predicate 缺失或 ordinal 越界时抛出
     */
    public LogicalFilter {
        if (input == null || predicates == null) {
            throw new DatabaseValidationException("logical filter requires non-empty predicates");
        }
        BoundExpressionValidation.validateCondition(
                predicates.condition(), input.table());
    }

    @Override
    public TableDefinition table() {
        return input.table();
    }
}
