package cn.zhangyis.db.sql.optimizer;

import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.type.SqlValue;
import cn.zhangyis.db.sql.optimizer.PredicateAnalyzer.BoundValue;
import cn.zhangyis.db.sql.optimizer.PredicateAnalyzer.ColumnConstraint;
import cn.zhangyis.db.sql.optimizer.PredicateAnalyzer.PredicateAnalysis;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;
import cn.zhangyis.db.sql.optimizer.logical.LogicalFilter;
import cn.zhangyis.db.sql.optimizer.logical.LogicalJoin;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;
import cn.zhangyis.db.sql.optimizer.logical.LogicalProject;
import cn.zhangyis.db.sql.optimizer.logical.LogicalTableModify;
import cn.zhangyis.db.sql.optimizer.logical.LogicalTableScan;
import cn.zhangyis.db.sql.optimizer.logical.LogicalValues;
import cn.zhangyis.db.sql.optimizer.logical.ModificationKind;
import cn.zhangyis.db.sql.optimizer.physical.IndexRange;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalFilter;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalInsert;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalJoinProbe;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalJoinProbeKind;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalJoinQuery;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPlan;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalProject;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalQuery;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalLimit;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSecondaryPrefixAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSortKey;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSortStrategy;
import cn.zhangyis.db.sql.optimizer.physical.PointAccessKind;
import cn.zhangyis.db.sql.optimizer.physical.RangeEndpoint;
import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * M4 确定性访问路径选择器。它只消费规则固定点与 PredicateAnalyzer 快照，
 * 并把 SELECT 输出为 project(filter(access))；本阶段不使用统计信息、memo 或动态规划。
 */
public final class HeuristicAccessPathSelector {
    /** Top-N 堆最多保留的行数；更大结果改用有界分堆 run，避免堆随 LIMIT 无界增长。 */
    private static final long TOP_N_RETAINED_ROWS = 4096L;
    /** 只负责从规范 PredicateSet 派生安全约束，不执行规则或选择索引。 */
    private final PredicateAnalyzer predicateAnalyzer;

    /**
     * 创建使用默认纯分析器的访问路径选择器。
     */
    public HeuristicAccessPathSelector() {
        this(new PredicateAnalyzer());
    }

    /**
     * 创建显式注入 predicate analyzer 的选择器。
     *
     * @param predicateAnalyzer 规则固定点之后的安全约束分析器
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException analyzer 缺失时抛出
     */
    public HeuristicAccessPathSelector(
            PredicateAnalyzer predicateAnalyzer) {
        if (predicateAnalyzer == null) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "predicate analyzer must not be null");
        }
        this.predicateAnalyzer = predicateAnalyzer;
    }

    /**
     * 识别当前规范关系形状并生成 point、secondary-prefix、range 或 clustered-scan 计划。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验逻辑根形状，只接受 project/filter/scan 或 table-modify 规范树。</li>
     *     <li>把 typed residual 按 column ordinal 求安全交集；SQL NULL 或可证明矛盾只设置 empty。</li>
     *     <li>按主键、唯一二级、最长连续前缀及 stable index id 的固定优先级选择访问路径。</li>
     *     <li>SELECT 构造 project/filter/access 物理树，DML 保持原子 physical command；
     *         两者都不打开事务、锁、PlanNode 或存储 cursor。</li>
     * </ol>
     *
     * @param logicalPlan Bound IR 转换出的单表逻辑计划；不得为 {@code null}
     * @return 与输入 exact table version 和 SQL 语义一致的不可变物理计划
     * @throws SqlOptimizationException 逻辑形状非法、DD 索引引用损坏或无安全执行路径时抛出
     */
    public PhysicalPlan select(LogicalPlan logicalPlan) {
        // 1、根节点是 optimizer/compiler 的协议，未知树形必须显式失败，不能静默全扫掩盖错误。
        if (logicalPlan == null) {
            throw new SqlOptimizationException("logical plan must not be null");
        }
        if (logicalPlan.root() instanceof LogicalProject project) {
            return optimizeSelect(project);
        }
        if (!(logicalPlan.root() instanceof LogicalTableModify modify)) {
            throw new SqlOptimizationException(
                    "unsupported logical root for heuristic access selection: "
                            + logicalPlan.root().getClass().getSimpleName());
        }
        // 2、INSERT 无 predicate；UPDATE/DELETE 的 filter 在各自分支完成约束分析。
        // 3、DML 仅在完整聚簇等值时降为 point，其它情况保留原子 range-mutation 计划。
        // 4、所有分支只产生数据，不创建任何运行期资源。
        return switch (modify.kind()) {
            case INSERT -> optimizeInsert(modify);
            case UPDATE -> optimizeUpdate(modify);
            case DELETE -> optimizeDelete(modify);
        };
    }

    /**
     * 把规范的单表 SELECT 关系树降为当前 Data Port 支持的三类读取计划。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拆出 project/filter/scan 并对全部 typed residual 求列约束交集和 empty 证明。</li>
     *     <li>仅对一致性读尝试完整主键或 logical-unique secondary point，避免丢失 locking intent。</li>
     *     <li>没有 point 候选时尝试当前专用的单列 non-unique equality prefix 计划。</li>
     *     <li>其余形状按最长连续前缀选择通用 range/full scan，并完整保留 residual 与 read mode。</li>
     * </ol>
     *
     * @param project converter 产生的 SELECT 根；输入必须是 filter(scan)
     * @return point、secondary prefix 或通用 range 物理计划
     * @throws SqlOptimizationException 关系形状、索引引用或 key 类型无法安全下沉时抛出
     */
    private PhysicalPlan optimizeSelect(LogicalProject project) {
        // 1、先验证规范树并形成唯一 PredicateAnalysis，后续候选不得重复解释 SQL predicate。
        LogicalFilter filter = requireFilter(project.input(), "SELECT");
        if (filter.input() instanceof LogicalJoin join) {
            return optimizeJoin(project, filter, join);
        }
        LogicalTableScan scan = requireScan(filter.input(), "SELECT");
        PredicateAnalysis analysis =
                predicateAnalyzer.analyze(filter.predicates());
        TableDefinition table = project.table();

        // 2、现有 point Data Port 只表达 consistent read；locking read 必须走携带 lockMode 的 range 计划。
        Optional<IndexDefinition> point = choosePointAccess(
                table, analysis.equalities().keySet());
        if (!analysis.empty() && point.isPresent()
                && scan.readMode() == SelectLockMode.CONSISTENT) {
            IndexDefinition access = point.orElseThrow(() ->
                    new SqlOptimizationException(
                            "point access candidate disappeared during deterministic selection"));
            rejectLobKey(table, access, "point SELECT");
            return query(project, filter, new PhysicalPointAccess(
                    table, access.id().value(),
                    access.clustered()
                            ? PointAccessKind.CLUSTERED_PRIMARY
                            : PointAccessKind.UNIQUE_SECONDARY,
                    keyValues(table, access, analysis.equalities())),
                    analysis.equalities());
        }
        // 3、普通二级索引的完整 logical equality 用 prefix range 保留多行结果与 locking mode。
        if (!analysis.empty() && point.isEmpty()) {
            Optional<IndexDefinition> secondary =
                    chooseSinglePartSecondary(table, analysis.exactEqualities());
            if (secondary.isPresent()) {
                IndexDefinition access = secondary.orElseThrow(() ->
                        new SqlOptimizationException(
                                "secondary range candidate disappeared during deterministic selection"));
                rejectLobKey(table, access, "secondary range SELECT");
                return query(project, filter,
                        new PhysicalSecondaryPrefixAccess(
                                table, access.id().value(),
                                keyValues(table, access,
                                        analysis.equalities()),
                                scan.readMode()),
                        analysis.equalities());
            }
        }

        // 4、comparison、复合前缀、locking point、empty 与无候选都归一到通用 range/full scan。
        RangeAccess access =
                chooseRangeAccess(table, analysis.constraints(), analysis.empty());
        return query(project, filter, new PhysicalRangeAccess(
                table, access.index().id().value(), access.range(),
                scan.readMode(), analysis.empty()), analysis.equalities());
    }

    /**
     * 为二表等值 INNER JOIN 选择确定性的左驱动 Nested Loop Join。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证左右输入均为一致性 table scan，并把 ON 规范为 relation 0 对 relation 1 的等值列。</li>
     *     <li>左表使用无界聚簇扫描；右表按 clustered、unique secondary、普通 secondary、stable id 顺序寻找单列完整索引。</li>
     *     <li>存在安全索引时生成 point/prefix 参数化 probe，否则生成每个 outer row 重开的聚簇全扫 probe。</li>
     *     <li>完整保留 ON、WHERE、扁平投影和排序；JOIN 顺序不声称满足 ORDER BY，必要时使用既有 Top-N/分堆排序。</li>
     * </ol>
     *
     * @param project JOIN SELECT 的公开投影、排序与限制根
     * @param filter 独立保存完整 WHERE residual 的过滤层
     * @param join 独立保存等值 ON 和 SQL 左右输入的逻辑连接层
     * @return 可由 JoinNode 直接实施的普通或索引 Nested Loop Join
     * @throws SqlOptimizationException ON 不是二表等值列、scan intent 非一致性读或聚簇全扫不安全时抛出
     */
    private PhysicalJoinQuery optimizeJoin(
            LogicalProject project,
            LogicalFilter filter,
            LogicalJoin join) {
        // 1、v1 不把 locking/current read 与多 cursor consistent scope 混用。
        LogicalTableScan leftScan =
                requireScan(join.left(), "INNER JOIN left");
        LogicalTableScan rightScan =
                requireScan(join.right(), "INNER JOIN right");
        if (leftScan.readMode() != SelectLockMode.CONSISTENT
                || rightScan.readMode()
                != SelectLockMode.CONSISTENT
                || !(join.condition().condition()
                instanceof BoundComparison comparison)
                || comparison.operator()
                != BoundComparisonOperator.EQUAL
                || !(comparison.left()
                instanceof BoundColumnReference first)
                || !(comparison.right()
                instanceof BoundColumnReference second)) {
            throw new SqlOptimizationException(
                    "INNER JOIN v1 requires two consistent scans and one column equality");
        }
        BoundColumnReference outerColumn;
        BoundColumnReference innerColumn;
        if (first.relationOrdinal() == 0
                && second.relationOrdinal() == 1) {
            outerColumn = first;
            innerColumn = second;
        } else if (first.relationOrdinal() == 1
                && second.relationOrdinal() == 0) {
            outerColumn = second;
            innerColumn = first;
        } else {
            throw new SqlOptimizationException(
                    "INNER JOIN ON must connect relation 0 to relation 1");
        }

        TableDefinition left = leftScan.table();
        TableDefinition right = rightScan.table();
        // 2、outer v1 固定 SQL 左表；该选择是确定性的，不冒充基于统计信息的 join reorder。
        if (left.primaryIndex().keyParts().stream()
                .anyMatch(part -> part.prefixBytes() != 0)
                || right.primaryIndex().keyParts().stream()
                .anyMatch(part -> part.prefixBytes() != 0)) {
            throw new SqlOptimizationException(
                    "INNER JOIN full scan does not support prefix clustered key");
        }
        PhysicalAccess outerAccess =
                new PhysicalRangeAccess(
                        left,
                        left.primaryIndex().id().value(),
                        IndexRange.unbounded(),
                        SelectLockMode.CONSISTENT,
                        false);
        List<IndexDefinition> candidates = right.indexes().stream()
                .filter(index -> index.keyParts().size() == 1
                        && index.keyParts().getFirst()
                        .prefixBytes() == 0
                        && index.keyParts().getFirst().columnId()
                        == innerColumn.columnId())
                .sorted(Comparator
                        .comparingInt((IndexDefinition index) ->
                                index.clustered() ? 0
                                        : index.unique() ? 1 : 2)
                        .thenComparingLong(index ->
                                index.id().value()))
                .toList();

        // 3、单列完整索引才能由一个 outer key 无损实例化；复合/前缀索引不会错误欠扫。
        PhysicalJoinProbe innerProbe;
        if (candidates.isEmpty()) {
            innerProbe = new PhysicalJoinProbe(
                    PhysicalJoinProbeKind.FULL_SCAN,
                    right,
                    right.primaryIndex().id().value(),
                    outerColumn.columnOrdinal(),
                    innerColumn.columnOrdinal(),
                    Optional.empty());
        } else {
            IndexDefinition chosen =
                    candidates.getFirst();
            if (chosen.clustered()
                    || chosen.unique()) {
                innerProbe = new PhysicalJoinProbe(
                        PhysicalJoinProbeKind.POINT,
                        right, chosen.id().value(),
                        outerColumn.columnOrdinal(),
                        innerColumn.columnOrdinal(),
                        Optional.of(chosen.clustered()
                                ? PointAccessKind.CLUSTERED_PRIMARY
                                : PointAccessKind.UNIQUE_SECONDARY));
            } else {
                innerProbe = new PhysicalJoinProbe(
                        PhysicalJoinProbeKind.SECONDARY_PREFIX,
                        right, chosen.id().value(),
                        outerColumn.columnOrdinal(),
                        innerColumn.columnOrdinal(),
                        Optional.empty());
            }
        }

        // 4、join 循环顺序不是全局排序证明；显式 ORDER BY 始终进入 blocking sort。
        List<PhysicalSortKey> orderBy =
                project.orderBy().stream()
                        .map(key -> new PhysicalSortKey(
                                key.columnOrdinal(),
                                key.direction()))
                        .toList();
        Optional<PhysicalLimit> limit =
                project.limit().map(value ->
                        new PhysicalLimit(
                                value.offset(), value.count()));
        PhysicalSortStrategy sortStrategy;
        if (orderBy.isEmpty()) {
            sortStrategy =
                    PhysicalSortStrategy.NONE;
        } else if (limit.isPresent()
                && limit.orElseThrow().retainedRows()
                <= TOP_N_RETAINED_ROWS) {
            sortStrategy =
                    PhysicalSortStrategy.TOP_N_HEAP;
        } else {
            sortStrategy =
                    PhysicalSortStrategy.PARTITIONED_HEAP_MERGE;
        }
        return new PhysicalJoinQuery(
                List.of(left, right),
                outerAccess, innerProbe,
                join.condition(), filter.predicates(),
                project.projectionOrdinals(),
                orderBy, limit, sortStrategy);
    }

    /**
     * 把已选访问叶包成唯一的 project(filter(access)) 查询树。
     *
     * @param project logical SELECT 的公开投影
     * @param filter 最终 SQL truth 的完整 residual
     * @param access point、secondary-prefix 或 range 访问叶
     * @param equalities WHERE 已证明的列等值，可跳过索引中的常量前缀
     * @return 不携带任何执行期资源的物理查询树
     */
    private static PhysicalQuery query(
            LogicalProject project, LogicalFilter filter,
            PhysicalAccess access, Map<Integer, SqlValue> equalities) {
        List<PhysicalSortKey> orderBy = project.orderBy().stream()
                .map(key -> new PhysicalSortKey(
                        key.columnOrdinal(), key.direction()))
                .toList();
        Optional<PhysicalLimit> limit = project.limit()
                .map(value -> new PhysicalLimit(value.offset(), value.count()));
        PhysicalSortStrategy strategy = chooseSortStrategy(
                project.table(), access, orderBy, limit, equalities);
        return new PhysicalQuery(new PhysicalProject(
                new PhysicalFilter(access, filter.predicates()),
                project.projectionOrdinals()),
                orderBy, limit, strategy);
    }

    /**
     * 在访问路径已经确定后选择排序策略。
     *
     * <ol>
     *     <li>无 ORDER BY 时返回 NONE；point 访问最多一行，任意排序都天然满足。</li>
     *     <li>按访问索引 key-part 顺序匹配排序键，允许跳过 WHERE 固定的完整等值前缀。</li>
     *     <li>索引不能满足时，只有 offset+count 不超过阈值才选择 Top-N 最大堆。</li>
     *     <li>其余请求进入固定内存 run 与最小堆归并，防止全量结果常驻内存。</li>
     * </ol>
     *
     * @param table exact table version
     * @param access 已选且校验完成的物理访问叶
     * @param orderBy 用户完整排序键
     * @param limit 可选最终结果边界
     * @param equalities WHERE 中可以证明的 typed 等值列
     * @return 可由 Executor 直接实施的确定性排序策略
     */
    private static PhysicalSortStrategy chooseSortStrategy(
            TableDefinition table, PhysicalAccess access,
            List<PhysicalSortKey> orderBy, Optional<PhysicalLimit> limit,
            Map<Integer, SqlValue> equalities) {
        // 1、空排序与单行 point 都不应创建 blocking SortNode。
        if (orderBy.isEmpty()) {
            return PhysicalSortStrategy.NONE;
        }
        if (access instanceof PhysicalPointAccess) {
            return PhysicalSortStrategy.INDEX;
        }
        // 2、只有完整 key part 且方向一致才是可靠物理顺序；prefix index 不提供全值排序证明。
        IndexDefinition index = table.indexes().stream()
                .filter(candidate ->
                        candidate.id().value() == access.accessIndexId())
                .findFirst()
                .orElseThrow(() -> new SqlOptimizationException(
                        "ORDER BY access index is missing from exact table version"));
        int orderPosition = 0;
        for (IndexKeyPart part : index.keyParts()) {
            ColumnDefinition column = columnById(
                    table, part.columnId(), "ORDER BY index proof");
            if (orderPosition < orderBy.size()) {
                PhysicalSortKey requested = orderBy.get(orderPosition);
                if (requested.columnOrdinal() == column.ordinal()
                        && requested.direction() == part.order()
                        && part.prefixBytes() == 0) {
                    orderPosition++;
                    continue;
                }
            }
            if (part.prefixBytes() == 0
                    && equalities.containsKey(column.ordinal())) {
                continue;
            }
            break;
        }
        if (orderPosition == orderBy.size()) {
            return PhysicalSortStrategy.INDEX;
        }
        // 3、Top-N 的堆容量由最终 offset+count 决定，阈值外必须切换分堆归并。
        if (limit.isPresent()
                && limit.orElseThrow().retainedRows() <= TOP_N_RETAINED_ROWS) {
            return PhysicalSortStrategy.TOP_N_HEAP;
        }
        // 4、无 LIMIT 或较大 LIMIT 都使用固定内存 run，不用全量 Java sort。
        return PhysicalSortStrategy.PARTITIONED_HEAP_MERGE;
    }

    /**
     * 把 modify(values) 转成单行 INSERT 物理计划。
     *
     * <ol>
     *     <li>验证 INSERT 没有被错误改写为 scan/filter 输入。</li>
     *     <li>复制 exact table version 的完整 typed row，不选择任何读取路径。</li>
     * </ol>
     *
     * @param modify INSERT 逻辑根
     * @return 完整单行写入计划
     * @throws SqlOptimizationException 输入不是 values 时抛出
     */
    private static PhysicalPlan optimizeInsert(LogicalTableModify modify) {
        // 1、INSERT 的 source 形状是 converter 与 optimizer 的稳定协议。
        if (!(modify.input() instanceof LogicalValues values)) {
            throw new SqlOptimizationException(
                    "logical INSERT requires a values input");
        }
        // 2、PhysicalInsert 继续执行防御性行宽校验，失败前不会打开写事务资源。
        return new PhysicalInsert(modify.table(), values.batch());
    }

    /**
     * 为 UPDATE 选择完整聚簇点修改或原子范围修改。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 filter(current-scan) 形状并计算 typed predicate 交集。</li>
     *     <li>仅当非 empty、全部为 equality 且列集合精确等于完整主键时构造 point UPDATE。</li>
     *     <li>其它情况选择最长安全 index prefix；empty 使用零访问标记。</li>
     *     <li>构造 range UPDATE 并保留全部 residual，Data Port 仍须先物化 identity 再修改。</li>
     * </ol>
     *
     * @param modify UPDATE 逻辑根
     * @return point 或原子 range UPDATE 计划
     * @throws SqlOptimizationException 逻辑形状或索引定义无法形成安全计划时抛出
     */
    private PhysicalPlan optimizeUpdate(LogicalTableModify modify) {
        // 1、UPDATE 必须来自 current-read scan；约束分析不删除任何 residual。
        LogicalFilter filter = requireFilter(modify.input(), "UPDATE");
        requireScan(filter.input(), "UPDATE");
        PredicateAnalysis analysis =
                predicateAnalyzer.analyze(filter.predicates());
        Optional<Map<Integer, SqlValue>> equalities = analysis.exactEqualities();
        // 2、额外 predicate 即使可由主键定位也不得降为 point，否则会丢失最终真值复核。
        if (!analysis.empty() && equalities.isPresent()) {
            Map<Integer, SqlValue> exact = equalities.orElseThrow(() ->
                    new SqlOptimizationException(
                            "UPDATE equality analysis lost its exact predicate map"));
            if (matchesExactKey(
                    modify.table(), modify.table().primaryIndex(), exact.keySet())) {
                return new PhysicalPointUpdate(
                        modify.table(), modify.assignmentOrdinals(),
                        modify.assignmentValues(),
                        keyValues(modify.table(), modify.table().primaryIndex(), exact));
            }
        }
        // 3、范围只缩小 candidate 集；同分候选按 stable id 保持重启可重复。
        RangeAccess access = chooseRangeAccess(
                modify.table(), analysis.constraints(), analysis.empty());
        // 4、完整 residual 随计划进入原子 range-mutation Data Port，避免 Halloween/partial mutation。
        return new PhysicalRangeUpdate(
                modify.table(), modify.assignmentOrdinals(), modify.assignmentValues(),
                access.index().id().value(), access.range(),
                filter.predicates(), analysis.empty());
    }

    /**
     * 为 DELETE 选择完整聚簇点标删或原子范围标删。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 filter(current-scan) 形状并分析全部 typed predicate。</li>
     *     <li>仅当非 empty 的 equality 集精确覆盖完整主键时构造 point DELETE。</li>
     *     <li>其它情况选择安全 range/full scan，empty 不获取行锁或写身份。</li>
     *     <li>范围计划携带全部 residual，由 Data Port 在同一 statement guard 内完成标删。</li>
     * </ol>
     *
     * @param modify DELETE 逻辑根
     * @return point 或原子 range DELETE 计划
     * @throws SqlOptimizationException 逻辑形状或索引定义无法形成安全计划时抛出
     */
    private PhysicalPlan optimizeDelete(LogicalTableModify modify) {
        // 1、DELETE 与 UPDATE 共用 current-read 输入协议和同一约束分析语义。
        LogicalFilter filter = requireFilter(modify.input(), "DELETE");
        requireScan(filter.input(), "DELETE");
        PredicateAnalysis analysis =
                predicateAnalyzer.analyze(filter.predicates());
        Optional<Map<Integer, SqlValue>> equalities = analysis.exactEqualities();
        // 2、只有 exact primary equality 才能跳过 range residual evaluation。
        if (!analysis.empty() && equalities.isPresent()) {
            Map<Integer, SqlValue> exact = equalities.orElseThrow(() ->
                    new SqlOptimizationException(
                            "DELETE equality analysis lost its exact predicate map"));
            if (matchesExactKey(
                    modify.table(), modify.table().primaryIndex(), exact.keySet())) {
                return new PhysicalPointDelete(
                        modify.table(), keyValues(
                        modify.table(), modify.table().primaryIndex(), exact));
            }
        }
        // 3、非 point 与 empty 都选择可验证的访问索引；empty 仅保留任意合法 primary identity。
        RangeAccess access = chooseRangeAccess(
                modify.table(), analysis.constraints(), analysis.empty());
        // 4、residual 是最终 SQL 真值权威，Data Port 不得只按候选范围删除。
        return new PhysicalRangeDelete(
                modify.table(), access.index().id().value(), access.range(),
                filter.predicates(), analysis.empty());
    }

    /**
     * 按固定优先级选择完整等值 point access。
     *
     * <ol>
     *     <li>完整无 prefix 聚簇主键优先，保持最短且无需回表的路径。</li>
     *     <li>否则在完整无 prefix logical-unique secondary 中取最小 stable id。</li>
     * </ol>
     *
     * @param table exact table version
     * @param predicateOrdinals 最外层正向 AND 可安全证明的 equality 列 ordinal 集
     * @return 可安全唯一定位逻辑记录的访问索引；没有时为空
     */
    private static Optional<IndexDefinition> choosePointAccess(
            TableDefinition table, Set<Integer> predicateOrdinals) {
        // 1、聚簇主键无需 secondary MVCC 回表，且行为不受 secondary 列表顺序影响。
        IndexDefinition primary = table.primaryIndex();
        if (keyCoveredBy(table, primary, predicateOrdinals)
                && primary.keyParts().stream().allMatch(part -> part.prefixBytes() == 0)) {
            return Optional.of(primary);
        }
        // 2、多个候选以持久 index id 排序，保证 catalog 重开后计划稳定。
        return table.indexes().stream()
                .filter(index -> !index.clustered() && index.unique())
                .filter(index -> index.keyParts().stream()
                        .allMatch(part -> part.prefixBytes() == 0))
                .filter(index -> keyCoveredBy(
                        table, index, predicateOrdinals))
                .min(Comparator.comparingLong(index -> index.id().value()));
    }

    /**
     * 判断安全 equality 集是否完整覆盖索引 logical key；允许额外 residual 列。
     *
     * <p>该判定只用于携带完整 residual 的一致性 point SELECT。Point DML 仍使用
     * {@link #matchesExactKey(TableDefinition, IndexDefinition, Set)}，防止跳过最终真值。</p>
     *
     * @param table exact table version
     * @param index 待证明可唯一定位的索引
     * @param equalityOrdinals Analyzer 从最外层正向 AND 提取的 equality 列
     * @return 每个完整 key part 都有必要 equality 证明时为 {@code true}
     */
    private static boolean keyCoveredBy(
            TableDefinition table, IndexDefinition index,
            Set<Integer> equalityOrdinals) {
        return index.keyParts().stream()
                .map(part -> columnById(
                        table, part.columnId(),
                        "point key coverage").ordinal())
                .allMatch(equalityOrdinals::contains);
    }

    /**
     * 选择当前专用 Data Port 支持的单列普通二级 equality prefix。
     *
     * @param table exact table version
     * @param exactEqualities 仅当全部 predicate 都是 equality 时存在
     * @return 最小 stable-id 的单列、无 prefix、non-unique secondary；无候选时为空
     */
    private static Optional<IndexDefinition> chooseSinglePartSecondary(
            TableDefinition table, Optional<Map<Integer, SqlValue>> exactEqualities) {
        if (exactEqualities.isEmpty()) {
            return Optional.empty();
        }
        Map<Integer, SqlValue> equalities = exactEqualities.orElseThrow(() ->
                new SqlOptimizationException(
                        "secondary selection lost its exact predicate map"));
        return table.indexes().stream()
                .filter(index -> !index.clustered() && !index.unique())
                .filter(index -> index.keyParts().size() == 1
                        && index.keyParts().getFirst().prefixBytes() == 0)
                .filter(index -> matchesExactKey(
                        table, index, equalities.keySet()))
                .min(Comparator.comparingLong(index -> index.id().value()));
    }

    /**
     * 判断 predicate 列集合是否与索引 logical key 列集合精确相等；声明顺序仅影响后续 key 排列。
     *
     * @param table exact table version
     * @param index 待比较访问索引
     * @param predicateOrdinals equality 列集合
     * @return 没有缺列或额外 residual 列时为 {@code true}
     */
    private static boolean matchesExactKey(
            TableDefinition table, IndexDefinition index, Set<Integer> predicateOrdinals) {
        if (predicateOrdinals.size() != index.keyParts().size()) {
            return false;
        }
        Set<Integer> keyOrdinals = index.keyParts().stream()
                .map(part -> columnById(table, part.columnId(), "index matching").ordinal())
                .collect(Collectors.toSet());
        return keyOrdinals.equals(predicateOrdinals);
    }

    /**
     * 按 index key-part 声明顺序重排 ordinal equality map。
     *
     * @param table exact table version
     * @param index 已证明被 equality 集完整覆盖的索引
     * @param equalities table ordinal 到 typed value 的映射
     * @return 可直接交给物理计划校验的不可变 logical key
     */
    private static List<SqlValue> keyValues(
            TableDefinition table, IndexDefinition index,
            Map<Integer, SqlValue> equalities) {
        return index.keyParts().stream()
                .map(part -> equalities.get(
                        columnById(table, part.columnId(), "key ordering").ordinal()))
                .toList();
    }

    /**
     * 选择最长连续 index prefix；同分按 stable index id，完全无候选时回退聚簇全扫。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>empty 计划只需要一个合法 index identity，固定返回聚簇索引的无界范围。</li>
     *     <li>逐个 DD index 提取安全连续前缀；prefix/LOB/无首列约束候选被跳过。</li>
     *     <li>按连续 key-part 得分降序、stable index id 升序选择唯一可重复结果。</li>
     *     <li>没有候选时验证聚簇索引可扫描并返回无界 full scan；不安全 prefix primary 显式失败。</li>
     * </ol>
     *
     * @param table metadata lease 固定的 exact table version
     * @param constraints 按 table ordinal 聚合的安全 predicate 约束
     * @param empty conjunction 已被证明不可能为 TRUE 时为 {@code true}
     * @return 所选 index、物理范围和连续前缀得分
     * @throws SqlOptimizationException 回退聚簇索引使用不受支持的 prefix key 时抛出
     */
    private static RangeAccess chooseRangeAccess(
            TableDefinition table, Map<Integer, ColumnConstraint> constraints,
            boolean empty) {
        // 1、empty 不会真正触达 Data Port scan，但计划仍携带 exact DD 中可验证的 index id。
        if (empty) {
            return new RangeAccess(table.primaryIndex(), IndexRange.unbounded(), 0);
        }
        RangeAccess best = null;
        // 2、每个索引独立提取连续前缀，任何候选都只能扩大、不能缩错 SQL 真值集合。
        for (IndexDefinition index : table.indexes()) {
            RangeAccess candidate = rangeForIndex(table, index, constraints);
            if (candidate == null) {
                continue;
            }
            // 3、得分相同使用持久 stable id，避免 DD 列表顺序改变计划。
            if (best == null || candidate.score() > best.score()
                    || candidate.score() == best.score()
                    && candidate.index().id().value() < best.index().id().value()) {
                best = candidate;
            }
        }
        if (best != null) {
            return best;
        }
        // 4、无 SARGable 首列时必须走完整聚簇扫描；prefix primary 尚无安全全扫 codec。
        IndexDefinition primary = table.primaryIndex();
        if (primary.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)) {
            throw new SqlOptimizationException(
                    "clustered full scan does not support prefix primary key");
        }
        return new RangeAccess(primary, IndexRange.unbounded(), 0);
    }

    /**
     * 将 equality prefix 与最多一个 range part 转成物理排序边界；DESC part 交换逻辑上下界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拒绝任何 prefix index，并初始化 equality prefix、上下界与连续得分。</li>
     *     <li>按声明 key-part 顺序消费约束：非 NULL equality 扩展连续 prefix，首个缺口立即停止。</li>
     *     <li>遇到首个 range part 时形成一侧或两侧 endpoint；DESC 将 SQL lower/upper 交换为物理顺序。</li>
     *     <li>没有约束返回非候选；纯 equality prefix 形成双侧闭区间，其余返回已冻结物理范围。</li>
     * </ol>
     *
     * @param table exact table version，用于把 index column id 解析为 ordinal/type
     * @param index 当前待评分 DD index
     * @param constraints typed predicate 的安全列约束
     * @return 可用候选及连续得分；无安全首列约束时为 {@code null}
     * @throws SqlOptimizationException index 引用了 exact table 中不存在的列时抛出
     */
    private static RangeAccess rangeForIndex(
            TableDefinition table, IndexDefinition index,
            Map<Integer, ColumnConstraint> constraints) {
        // 1、当前选择器不把 prefix-byte 编码近似成完整 SQL 比较；该索引不参与候选。
        if (index.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)) {
            return null;
        }
        ArrayList<SqlValue> equalityPrefix = new ArrayList<>();
        RangeEndpoint lower = null;
        RangeEndpoint upper = null;
        int score = 0;
        for (IndexKeyPart part : index.keyParts()) {
            // 2、只消费从首 key part 开始的连续约束；LOB/JSON 尚无稳定 SQL key comparator。
            ColumnDefinition column = columnById(table, part.columnId(), "range access");
            if (isLobKey(column.type().typeId())) {
                return null;
            }
            ColumnConstraint constraint = constraints.get(column.ordinal());
            if (constraint == null) {
                break;
            }
            if (constraint.hasEquality()
                    && !(constraint.equality() instanceof SqlValue.NullValue)) {
                equalityPrefix.add(constraint.equality());
                score++;
                continue;
            }
            // 3、一个 range part 终止连续 prefix；DESC 索引的物理方向与 SQL 值序相反。
            RangeEndpoint logicalLower = endpoint(equalityPrefix, constraint.lower());
            RangeEndpoint logicalUpper = endpoint(equalityPrefix, constraint.upper());
            if (logicalLower == null && logicalUpper == null) {
                break;
            }
            if (part.order() == IndexOrder.ASC) {
                lower = logicalLower;
                upper = logicalUpper;
            } else {
                lower = logicalUpper;
                upper = logicalLower;
            }
            score++;
            break;
        }
        // 4、零分不是候选；纯 equality prefix 用双侧闭 endpoint 覆盖 secondary clustered suffix。
        if (score == 0) {
            return null;
        }
        if (lower == null && upper == null && !equalityPrefix.isEmpty()) {
            lower = new RangeEndpoint(equalityPrefix, true);
            upper = new RangeEndpoint(equalityPrefix, true);
        }
        return new RangeAccess(
                index, new IndexRange(
                Optional.ofNullable(lower), Optional.ofNullable(upper)), score);
    }

    /**
     * 把 equality prefix 与一个 range value 拼接为物理 endpoint。
     *
     * @param prefix 已按 index 顺序排列的 equality 前缀
     * @param value 当前 range key-part；不存在表示该侧无界
     * @return 连续前缀 endpoint；无界时为 {@code null}
     */
    private static RangeEndpoint endpoint(List<SqlValue> prefix, BoundValue value) {
        if (value == null) {
            return null;
        }
        ArrayList<SqlValue> keys = new ArrayList<>(prefix);
        keys.add(value.value());
        return new RangeEndpoint(keys, value.inclusive());
    }

    /**
     * 校验规范逻辑树中的 filter 层，避免规则对未知节点形状作隐式语义假设。
     *
     * @param input project 或 table-modify 的直接输入
     * @param operation 用于诊断的 SQL 操作名
     * @return 已验证的逻辑过滤节点
     * @throws SqlOptimizationException input 不是 {@link LogicalFilter} 时抛出
     */
    private static LogicalFilter requireFilter(Object input, String operation) {
        if (input instanceof LogicalFilter filter) {
            return filter;
        }
        throw new SqlOptimizationException(
                "logical " + operation + " requires a filter input");
    }

    /**
     * 校验规范逻辑树中的叶扫描层；当前选择器不允许悄悄忽略其它关系节点。
     *
     * @param input filter 的直接输入
     * @param operation 用于诊断的 SQL 操作名
     * @return 已验证、仍保留 read intent 的单表扫描
     * @throws SqlOptimizationException input 不是 {@link LogicalTableScan} 时抛出
     */
    private static LogicalTableScan requireScan(Object input, String operation) {
        if (input instanceof LogicalTableScan scan) {
            return scan;
        }
        throw new SqlOptimizationException(
                "logical " + operation + " requires a table scan input");
    }

    /**
     * 在计划绑定的 exact table version 内解析索引 key-part 的 column identity。
     *
     * @param table metadata lease 固定的表定义
     * @param columnId DD index key-part 持久化的 column id
     * @param operation 用于诊断的规划阶段
     * @return 属于同一 table version 的列定义
     * @throws SqlOptimizationException index 引用了该版本不存在的列时抛出
     */
    private static ColumnDefinition columnById(
            TableDefinition table, long columnId, String operation) {
        return table.columns().stream()
                .filter(column -> column.columnId() == columnId)
                .findFirst()
                .orElseThrow(() -> new SqlOptimizationException(
                        operation + " index references missing DD column " + columnId));
    }

    /**
     * 拒绝当前 key codec 不能安全编码的 LOB/JSON 点查索引。
     *
     * @param table index 所属 exact table version
     * @param index point 候选索引
     * @param operation 用于诊断的 SELECT 物理形状
     * @throws SqlOptimizationException index 引用缺列或包含 LOB/JSON key 时抛出
     */
    private static void rejectLobKey(
            TableDefinition table, IndexDefinition index, String operation) {
        for (IndexKeyPart part : index.keyParts()) {
            ColumnDefinition column = columnById(table, part.columnId(), operation);
            if (isLobKey(column.type().typeId())) {
                throw new SqlOptimizationException(
                        operation + " does not support LOB/JSON index key");
            }
        }
    }

    /**
     * 判断 DD 类型是否需要当前索引访问计划尚未提供的 LOB/JSON key codec。
     *
     * @param type exact column 的 DD 类型
     * @return LOB family 或 JSON 时为 {@code true}
     */
    private static boolean isLobKey(DictionaryTypeId type) {
        return switch (type) {
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
                    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> true;
            default -> false;
        };
    }

    /**
     * 一个可执行索引范围候选。
     *
     * @param index exact table version 中的访问索引
     * @param range 按该索引物理顺序表达的连续范围
     * @param score 从首 key-part 开始连续消费的列数
     */
    private record RangeAccess(IndexDefinition index, IndexRange range, int score) {
    }
}
