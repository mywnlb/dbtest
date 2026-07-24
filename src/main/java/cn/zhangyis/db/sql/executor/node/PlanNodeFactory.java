package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.expression.ExpressionEvaluator;
import cn.zhangyis.db.sql.executor.storage.SqlDataAccessPort;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalQuery;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalJoinQuery;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSecondaryPrefixAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSortStrategy;

import java.util.List;

/**
 * 把不可变 PhysicalQuery 映射为每语句私有 PlanNode tree；Optimizer 不依赖任何 runtime node。
 */
public final class PlanNodeFactory {
    /** 所有 access node 共用的窄数据端口。 */
    private final SqlDataAccessPort storage;
    /** FilterNode 使用的无状态三值解释器。 */
    private final ExpressionEvaluator evaluator;
    /** SortNode 的临时根和单 run 内存上界。 */
    private final SortExecutionConfig sortConfig;

    /**
     * @param storage Executor 数据访问端口
     * @param evaluator canonical expression evaluator
     * @throws DatabaseValidationException 任一协作者缺失时抛出
     */
    public PlanNodeFactory(
            SqlDataAccessPort storage, ExpressionEvaluator evaluator) {
        this(storage, evaluator, SortExecutionConfig.defaults());
    }

    /**
     * 创建可注入排序资源边界的节点工厂，供实例配置和小容量分堆测试使用。
     *
     * @param storage Executor 数据访问端口
     * @param evaluator canonical expression evaluator
     * @param sortConfig 临时目录与单 run 最大行数
     * @throws DatabaseValidationException 任一协作者缺失时抛出
     */
    public PlanNodeFactory(
            SqlDataAccessPort storage, ExpressionEvaluator evaluator,
            SortExecutionConfig sortConfig) {
        if (storage == null || evaluator == null || sortConfig == null) {
            throw new DatabaseValidationException(
                    "plan node factory collaborators must not be null");
        }
        this.storage = storage;
        this.evaluator = evaluator;
        this.sortConfig = sortConfig;
    }

    /**
     * 按 access→filter→sort→limit→project 顺序构造运行期树；不打开任何节点。
     *
     * <ol>
     *     <li>校验完整 PhysicalQuery，避免创建无 metadata 根的部分节点。</li>
     *     <li>把 sealed PhysicalAccess 映射为唯一具体 AccessNode。</li>
     *     <li>先包裹 FilterNode；仅在策略要求时创建 SortNode，索引有序路径不创建空转排序节点。</li>
     *     <li>在排序结果外包裹可选 LimitNode，最后创建 ProjectionNode，返回状态为 NEW 的根。</li>
     * </ol>
     *
     * @param query Optimizer 产生的完整物理查询
     * @return 状态为 NEW 的 ProjectionNode 根
     * @throws DatabaseValidationException query 缺失时抛出；不会创建 cursor 或其它运行资源
     */
    public PlanNode create(PhysicalQuery query) {
        // 1、参数错误早于任何 stateful 节点发布。
        if (query == null) {
            throw new DatabaseValidationException(
                    "physical query must not be null");
        }
        var project = query.root();
        var filter = project.input();
        // 2、sealed 映射使新增访问类型必须同步定义运行语义。
        PlanNode access = accessNode(filter.input());
        // 3、Filter 仍拥有最终 SQL truth；Sort 只能消费完整 table row，不能被投影裁掉排序列。
        PlanNode ordered = new FilterNode(
                access, filter.predicates(), evaluator);
        if (query.sortStrategy() == PhysicalSortStrategy.TOP_N_HEAP
                || query.sortStrategy()
                == PhysicalSortStrategy.PARTITIONED_HEAP_MERGE) {
            ordered = new SortNode(
                    ordered, query.orderBy(), query.sortStrategy(),
                    query.limit(), sortConfig);
        }
        // 4、Limit 消费已排序行并可在 count=0 时阻止打开 child；Projection 保持公开结果根。
        PlanNode limited = ordered;
        if (query.limit().isPresent()) {
            limited = new LimitNode(
                    limited, query.limit().orElseThrow());
        }
        return new ProjectionNode(
                limited, project.projectionOrdinals());
    }

    /**
     * 按 outer-access → NestedLoopJoin(参数化 inner) → WHERE filter → sort → limit
     * → project 构造二表运行树；所有 inner 节点都在 outer key 出现时才创建。
     *
     * @param query Optimizer 产生的不可变二表 JOIN 计划
     * @return 状态为 NEW 的 ProjectionNode 根
     */
    public PlanNode create(
            PhysicalJoinQuery query) {
        if (query == null) {
            throw new DatabaseValidationException(
                    "physical JOIN query must not be null");
        }
        PlanNode outer =
                accessNode(query.outerAccess());
        List<cn.zhangyis.db.sql.executor.ResultColumn>
                innerColumns =
                query.tables().getLast().columns().stream()
                        .map(column ->
                                new cn.zhangyis.db.sql.executor.ResultColumn(
                                        column.name().displayName(),
                                        column.type()))
                        .toList();
        PlanNode joined = new NestedLoopJoinNode(
                outer,
                key -> accessNode(
                        query.innerProbe()
                                .accessFor(key)),
                query.innerProbe()
                        .outerColumnOrdinal(),
                query.joinPredicates(),
                evaluator, innerColumns);
        PlanNode ordered = new FilterNode(
                joined, query.predicates(),
                evaluator);
        if (query.sortStrategy()
                == PhysicalSortStrategy.TOP_N_HEAP
                || query.sortStrategy()
                == PhysicalSortStrategy
                .PARTITIONED_HEAP_MERGE) {
            ordered = new SortNode(
                    ordered, query.orderBy(),
                    query.sortStrategy(),
                    query.limit(), sortConfig);
        }
        PlanNode limited = ordered;
        if (query.limit().isPresent()) {
            limited = new LimitNode(
                    limited,
                    query.limit().orElseThrow());
        }
        return new ProjectionNode(
                limited,
                query.projectionOrdinals());
    }

    /** 将 sealed PhysicalAccess 穷尽映射为对应 storage node。 */
    private PlanNode accessNode(PhysicalAccess access) {
        return switch (access) {
            case PhysicalPointAccess point ->
                    new PointAccessNode(storage, point);
            case PhysicalSecondaryPrefixAccess secondary ->
                    new SecondaryPrefixAccessNode(storage, secondary);
            case PhysicalRangeAccess range ->
                    new RangeAccessNode(storage, range);
        };
    }
}
