package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.expression.BoundExpressionValidation;

import java.util.List;

/**
 * 保持 SQL 左右输入顺序的二表 INNER JOIN。ON 是独立于上层 WHERE 的最终连接真值，
 * 本节点不决定驱动表交换、索引 probe 或运行算法。
 *
 * @param left SQL FROM 左输入
 * @param right SQL JOIN 右输入
 * @param condition 完成 relation-aware 绑定的等值 ON
 */
public record LogicalJoin(
        RelNode left, RelNode right,
        PredicateSet condition) implements RelNode {

    public LogicalJoin {
        if (left == null || right == null || condition == null
                || left.tables().size() != 1
                || right.tables().size() != 1) {
            throw new DatabaseValidationException(
                    "logical INNER JOIN requires two single-relation inputs");
        }
        BoundExpressionValidation.validateCondition(
                condition.condition(),
                List.of(left.table(), right.table()));
    }

    @Override
    public List<TableDefinition> tables() {
        return List.of(left.table(), right.table());
    }
}
