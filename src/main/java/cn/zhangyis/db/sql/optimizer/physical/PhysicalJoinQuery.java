package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.expression.BoundExpressionValidation;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * 二表 INNER JOIN 的不可变物理根。outer 固定为 SQL 左表，inner probe 每个 outer row
 * 重新实例化；ON 与 WHERE 分离，排序/限制/投影都作用于扁平 joined schema。
 */
public record PhysicalJoinQuery(
        List<TableDefinition> tables,
        PhysicalAccess outerAccess,
        PhysicalJoinProbe innerProbe,
        PredicateSet joinPredicates,
        PredicateSet predicates,
        List<Integer> projectionOrdinals,
        List<PhysicalSortKey> orderBy,
        Optional<PhysicalLimit> limit,
        PhysicalSortStrategy sortStrategy) implements PhysicalPlan {

    /**
     * 校验物理连接树仍完全覆盖 Binder 语义。
     *
     * <ol>
     *     <li>要求两个 exact tables，outer/probe 分别绑定左/右表。</li>
     *     <li>校验 ON/WHERE 的 relation-aware stable column identity。</li>
     *     <li>验证扁平投影和排序位置，并保持重复投影拒绝语义。</li>
     *     <li>核对排序策略与 LIMIT；成功后冻结全部容器，不创建运行期资源。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 表、表达式、schema 位置或排序策略不一致时抛出
     */
    public PhysicalJoinQuery {
        // 1、左右 exact metadata 身份必须贯穿 optimizer/executor。
        if (tables == null || tables.size() != 2
                || tables.stream().anyMatch(java.util.Objects::isNull)
                || outerAccess == null || innerProbe == null
                || joinPredicates == null || predicates == null
                || projectionOrdinals == null
                || projectionOrdinals.isEmpty()
                || orderBy == null || limit == null
                || sortStrategy == null
                || outerAccess.table() != tables.getFirst()
                || innerProbe.table() != tables.getLast()
                || innerProbe.outerColumnOrdinal()
                >= tables.getFirst().columns().size()) {
            throw new DatabaseValidationException(
                    "invalid physical INNER JOIN fields");
        }
        // 2、规则不能把 ON/WHERE 的 relation identity 改成扁平 ordinal 猜测。
        BoundExpressionValidation.validateCondition(
                joinPredicates.condition(), tables);
        BoundExpressionValidation.validateCondition(
                predicates.condition(), tables);
        int width = tables.stream()
                .mapToInt(table -> table.columns().size()).sum();
        // 3、公开投影仍保持用户顺序，但每个位置必须属于 joined schema。
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projectionOrdinals) {
            if (ordinal == null || ordinal < 0
                    || ordinal >= width
                    || !unique.add(ordinal)) {
                throw new DatabaseValidationException(
                        "physical JOIN projection ordinal is invalid or duplicate");
            }
        }
        for (PhysicalSortKey key : orderBy) {
            if (key == null || key.columnOrdinal() >= width) {
                throw new DatabaseValidationException(
                        "physical JOIN sort key is outside joined schema");
            }
        }
        // 4、JOIN v1 不声称索引有序跨 outer/inner 循环成立。
        if (orderBy.isEmpty()
                != (sortStrategy == PhysicalSortStrategy.NONE)
                || sortStrategy == PhysicalSortStrategy.INDEX
                || sortStrategy == PhysicalSortStrategy.TOP_N_HEAP
                && limit.isEmpty()) {
            throw new DatabaseValidationException(
                    "physical JOIN sort strategy does not match ORDER BY/LIMIT");
        }
        tables = List.copyOf(tables);
        projectionOrdinals =
                List.copyOf(projectionOrdinals);
        orderBy = List.copyOf(orderBy);
    }
}
