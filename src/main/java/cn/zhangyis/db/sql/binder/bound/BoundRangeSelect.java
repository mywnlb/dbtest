package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.dd.domain.TableDefinition;

import java.util.List;

/**
 * comparison/composite/full-scan SELECT 计划。索引边界只缩小候选集，predicates 是最终真值权威。
 *
 * @param table statement metadata lease 固定的 exact table version
 * @param projectionOrdinals 用户投影顺序
 * @param accessIndexId 确定性选择的访问索引；full scan 时为聚簇索引
 * @param indexRange 可无界、开放或闭合的物理排序范围
 * @param predicates 已完成类型转换的全部 residual conjunction
 * @param lockMode 一致性读或 current locking read
 * @param empty Binder 已证明 SQL 三值 conjunction 不可能为 TRUE
 */
public record BoundRangeSelect(TableDefinition table, List<Integer> projectionOrdinals,
                               long accessIndexId, BoundIndexRange indexRange,
                               List<BoundRowPredicate> predicates, SelectLockMode lockMode,
                               boolean empty) implements BoundStatement {
    public BoundRangeSelect {
        BoundRangePlanValidation.validate(table, accessIndexId, indexRange, predicates);
        BoundRangePlanValidation.validateProjection(table, projectionOrdinals);
        if (lockMode == null) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "bound range SELECT lock mode must not be null");
        }
        projectionOrdinals = List.copyOf(projectionOrdinals);
        predicates = List.copyOf(predicates);
    }
}
