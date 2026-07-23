package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;

import java.util.List;

/**
 * comparison、复合前缀或聚簇全扫 SELECT 计划。indexRange 只缩小候选集合，predicates 始终是
 * 最终 SQL 真值权威。
 *
 * @param table exact DD table version
 * @param projectionOrdinals 用户投影顺序
 * @param accessIndexId 访问索引稳定 id；全扫时为聚簇索引
 * @param indexRange 可无界、开放或闭合的物理范围
 * @param predicates 完成类型转换的完整 boolean residual
 * @param lockMode 一致性读或 current locking read
 * @param empty Optimizer 已证明 conjunction 不可能为 TRUE
 */
public record PhysicalRangeSelect(
        TableDefinition table, List<Integer> projectionOrdinals, long accessIndexId,
        IndexRange indexRange, PredicateSet predicates,
        SelectLockMode lockMode, boolean empty) implements PhysicalPlan {
    /**
     * 校验并冻结通用范围查询的访问、residual、投影与读意图。
     *
     * @throws DatabaseValidationException 任一字段与 exact table/index 版本不一致时抛出
     */
    public PhysicalRangeSelect {
        PhysicalPlanValidation.validateRange(table, accessIndexId, indexRange, predicates);
        PhysicalPlanValidation.validateProjection(table, projectionOrdinals);
        if (lockMode == null) {
            throw new DatabaseValidationException(
                    "physical range SELECT lock mode must not be null");
        }
        projectionOrdinals = List.copyOf(projectionOrdinals);
    }
}
