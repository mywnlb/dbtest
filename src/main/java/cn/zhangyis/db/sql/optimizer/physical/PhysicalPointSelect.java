package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 已选择聚簇主键或唯一二级键的等值点查。
 *
 * @param table exact DD table version
 * @param projectionOrdinals 用户投影列 ordinal，顺序即结果列顺序
 * @param accessIndexId 所选 DD index 的稳定 id
 * @param accessKind 聚簇主键或唯一二级访问种类
 * @param keyValues 按所选 index key-part 顺序排列的完整 typed key
 * @param predicates 完整 SQL residual；unique secondary 回表后必须重新求值
 */
public record PhysicalPointSelect(TableDefinition table, List<Integer> projectionOrdinals,
                                  long accessIndexId, PointAccessKind accessKind,
                                  List<SqlValue> keyValues,
                                  PredicateSet predicates) implements PhysicalPlan {

    /**
     * 校验并冻结点查计划，防止错误索引种类或不完整 key 下沉到 Data Port。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验必填字段，任何缺失都在执行资源创建前失败。</li>
     *     <li>在 exact table version 中解析索引，并核对完整、无 prefix 的 key 形状。</li>
     *     <li>核对访问种类与索引的 clustered/unique 属性，阻止错误回表路径。</li>
     *     <li>验证投影，并证明每个 access key 都来自完整 residual 的相同 typed equality；
     *         最后复制集合隔离调用方修改。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 计划字段、索引、key 或投影不满足物理契约时抛出
     */
    public PhysicalPointSelect {
        // 1、Java null 不属于 SQL 值域，不能进入执行阶段。
        if (table == null || projectionOrdinals == null || projectionOrdinals.isEmpty()
                || accessKind == null || keyValues == null || predicates == null
                || keyValues.stream().anyMatch(java.util.Objects::isNull)
                || keyValues.stream().anyMatch(SqlValue.NullValue.class::isInstance)) {
            throw new DatabaseValidationException("invalid physical point SELECT fields");
        }
        // 2、点查必须完整覆盖当前 DD 版本中的无 prefix logical key。
        IndexDefinition index = PhysicalPlanValidation.requireIndex(table, accessIndexId);
        if (keyValues.size() != index.keyParts().size()
                || index.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)) {
            throw new DatabaseValidationException(
                    "physical point SELECT requires complete non-prefix index key");
        }
        PhysicalPlanValidation.rejectLobKey(table, index);
        // 3、访问种类决定 Data Port 是否回表，不能与 DD 属性错配。
        boolean validKind = switch (accessKind) {
            case CLUSTERED_PRIMARY -> index.clustered() && index.unique();
            case UNIQUE_SECONDARY -> !index.clustered() && index.unique();
        };
        if (!validKind) {
            throw new DatabaseValidationException(
                    "physical point SELECT access kind/index metadata mismatch");
        }
        // 4、投影顺序可观察但不可重复；point key 必须由 residual 证明，防止 under-scan。
        PhysicalPlanValidation.validateProjection(table, projectionOrdinals);
        PhysicalPlanValidation.validatePointResidual(
                table, index, keyValues, predicates);
        projectionOrdinals = List.copyOf(projectionOrdinals);
        keyValues = List.copyOf(keyValues);
    }
}
