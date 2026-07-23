package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 单列普通二级索引 logical key 的物理 prefix-range 计划。存储层在 logical key 后追加聚簇后缀，
 * 因而一次逻辑等值可以返回多行。
 *
 * @param table exact DD table version
 * @param projectionOrdinals 公开结果投影 ordinal
 * @param accessIndexId 普通二级索引稳定 id
 * @param logicalKeyValues 按 logical key-part 顺序排列的 typed values
 * @param lockMode 一致性读或 current locking read
 * @param predicates 完整 SQL residual；secondary candidate 回表后必须重新求值
 */
public record PhysicalSecondaryRangeSelect(
        TableDefinition table, List<Integer> projectionOrdinals, long accessIndexId,
        List<SqlValue> logicalKeyValues, SelectLockMode lockMode,
        PredicateSet predicates) implements PhysicalPlan {

    /**
     * 校验当前教学切片支持的单列、无 prefix、non-unique secondary 形状。
     *
     * <ol>
     *     <li>拒绝缺失字段、Java null 与 SQL NULL key，避免执行期猜测 SQL 值域。</li>
     *     <li>在 exact table version 中解析索引并校验单列、无 prefix、普通二级形状。</li>
     *     <li>校验投影，并证明 logical key 来自完整 residual 的相同 typed equality。</li>
     *     <li>冻结公开集合，避免调用方在 Executor 运行期间篡改计划。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 字段、索引形状、key 或投影不满足契约时抛出
     */
    public PhysicalSecondaryRangeSelect {
        // 1、Java null/SQL NULL 不能形成当前 prefix-range 的稳定 logical key。
        if (table == null || projectionOrdinals == null || projectionOrdinals.isEmpty()
                || logicalKeyValues == null
                || logicalKeyValues.stream().anyMatch(java.util.Objects::isNull)
                || logicalKeyValues.stream().anyMatch(SqlValue.NullValue.class::isInstance)
                || lockMode == null || predicates == null) {
            throw new DatabaseValidationException(
                    "invalid physical secondary range SELECT fields");
        }
        // 2、专用路径只消费单列普通二级索引；其它形状由通用 range plan 承接。
        IndexDefinition index = PhysicalPlanValidation.requireIndex(table, accessIndexId);
        if (index.clustered() || index.unique() || index.keyParts().size() != 1
                || index.keyParts().getFirst().prefixBytes() != 0
                || logicalKeyValues.size() != 1) {
            throw new DatabaseValidationException(
                    "physical secondary range SELECT requires one-part non-prefix non-unique secondary");
        }
        PhysicalPlanValidation.rejectLobKey(table, index);
        // 3、projection 与最终 truth 都相对同一 exact table version，并证明 access key 安全。
        PhysicalPlanValidation.validateProjection(table, projectionOrdinals);
        PhysicalPlanValidation.validatePointResidual(
                table, index, logicalKeyValues, predicates);
        // 4、执行期只读取冻结集合，不共享调用方可变容器。
        projectionOrdinals = List.copyOf(projectionOrdinals);
        logicalKeyValues = List.copyOf(logicalKeyValues);
    }
}
