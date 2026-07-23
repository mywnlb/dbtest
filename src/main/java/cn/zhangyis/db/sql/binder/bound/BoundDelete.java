package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundExpressionValidation;

/**
 * 单表 DELETE 的纯语义绑定结果；Optimizer 决定主键点删除或原子范围删除。
 *
 * @param table statement metadata lease 固定的 exact table version
 * @param condition 完成名称解析和类型转换的完整 WHERE condition
 */
public record BoundDelete(TableDefinition table, BoundExpression condition)
        implements BoundRelationalStatement {

    /**
     * 校验并冻结 DELETE predicate。
     *
     * <ol>
     *     <li>拒绝缺失 exact table version 或 condition。</li>
     *     <li>递归核对 condition 的 stable column identity、ordinal 与完整 DD type。</li>
     * </ol>
     *
     * @throws DatabaseValidationException table、predicate 容器缺失或 ordinal 越界时抛出
     */
    public BoundDelete {
        // 1、DELETE 不允许以缺失 predicate 的方式隐式扩张为全表删除。
        if (table == null || condition == null) {
            throw new DatabaseValidationException("invalid semantic bound DELETE fields");
        }
        // 2、物理选路之前先闭合 exact metadata version 不变量。
        BoundExpressionValidation.validateCondition(condition, table);
    }
}
