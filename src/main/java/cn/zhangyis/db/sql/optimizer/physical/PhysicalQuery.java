package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;

import java.util.List;
import java.util.Optional;

/**
 * SELECT 的物理计划根。M4 首个生产形状固定为 project(filter(access))，Executor 通过
 * 独立 builder 映射为每语句私有的 stateful PlanNode tree。
 *
 * @param root 不可变物理投影根
 * @param orderBy 投影前完整 table row 上应用的排序键
 * @param limit 投影后应用的最终 offset/count
 * @param sortStrategy Optimizer 已证明的索引、Top-N 或分堆归并策略
 */
public record PhysicalQuery(
        PhysicalProject root, List<PhysicalSortKey> orderBy,
        Optional<PhysicalLimit> limit,
        PhysicalSortStrategy sortStrategy) implements PhysicalPlan {

    /**
     * 拒绝缺失根，避免 Executor 在资源创建后才发现不完整 operator tree。
     *
     * <ol>
     *     <li>确认 Project 根存在，并沿固定形状取得 Filter/Access。</li>
     *     <li>point/secondary 以父 Filter 证明完整 key 必要条件；range 只需保留完整 residual。</li>
     * </ol>
     *
     * @throws DatabaseValidationException root 缺失或 point/secondary key 未被 Filter 证明时抛出
     */
    public PhysicalQuery {
        // 1、根缺失必须早于跨算子 proof 和任何执行期资源创建失败。
        if (root == null || orderBy == null || limit == null || sortStrategy == null) {
            throw new DatabaseValidationException("physical query root must not be null");
        }
        orderBy = List.copyOf(orderBy);
        if (orderBy.isEmpty() != (sortStrategy == PhysicalSortStrategy.NONE)) {
            throw new DatabaseValidationException(
                    "physical sort strategy must match ORDER BY presence");
        }
        for (PhysicalSortKey key : orderBy) {
            if (key == null || key.columnOrdinal() >= root.table().columns().size()) {
                throw new DatabaseValidationException(
                        "physical sort key is outside table schema");
            }
        }
        if (sortStrategy == PhysicalSortStrategy.TOP_N_HEAP && limit.isEmpty()) {
            throw new DatabaseValidationException(
                    "Top-N sort requires a physical LIMIT");
        }
        PhysicalFilter filter = root.input();
        // 2、access 只持定位 key，最终 SQL truth 必须由独立 Filter 提供证明。
        switch (filter.input()) {
            case PhysicalPointAccess point -> PhysicalPlanValidation.validatePointResidual(
                    point.table(),
                    PhysicalPlanValidation.requireIndex(
                            point.table(), point.accessIndexId()),
                    point.keyValues(), filter.predicates());
            case PhysicalSecondaryPrefixAccess secondary ->
                    PhysicalPlanValidation.validatePointResidual(
                            secondary.table(),
                            PhysicalPlanValidation.requireIndex(
                                    secondary.table(),
                                    secondary.accessIndexId()),
                            secondary.logicalKeyValues(),
                            filter.predicates());
            case PhysicalRangeAccess ignored -> {
                // Range 只缩小候选；Filter 已保存且校验完整 residual，不需要 point 必要条件证明。
            }
        }
    }

    @Override
    public TableDefinition table() {
        return root.table();
    }

    /**
     * 保留排序能力引入前的物理查询构造形状。
     */
    public PhysicalQuery(PhysicalProject root) {
        this(root, List.of(), Optional.empty(), PhysicalSortStrategy.NONE);
    }
}
