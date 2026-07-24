package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.exception.SqlExecutionException;
import cn.zhangyis.db.sql.executor.row.MaterializedSqlRowView;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalLimit;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSortKey;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSortStrategy;
import cn.zhangyis.db.sql.type.SqlValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证 Top-N、分堆 run 归并、稳定 tie-break 和 LIMIT 的资源边界。 */
class SortNodeTest {
    @TempDir
    Path directory;

    /**
     * Top-N 最大堆只保留 offset+count 个最佳候选，外层 Limit 再精确跳过 offset；
     * 因此不会把被 LIMIT 永远遮蔽的尾部行保留在堆中。
     */
    @Test
    void retainsOnlyTopNWindowAndAppliesOffsetAfterSort() {
        TestRowsNode source = rows(List.of(
                row(5, 50), row(1, 10), row(4, 40),
                row(2, 20), row(3, 30)));
        PhysicalLimit limit = new PhysicalLimit(1, 2);
        PlanNode root = new LimitNode(new SortNode(
                source, List.of(new PhysicalSortKey(0, IndexOrder.ASC)),
                PhysicalSortStrategy.TOP_N_HEAP, Optional.of(limit),
                new SortExecutionConfig(directory, 2)), limit);

        assertEquals(List.of(2L, 3L), pullFirstColumn(root));
        assertEquals(1, source.openCalls);
        assertEquals(1, source.closeCalls);
    }

    /**
     * 分堆排序以两行为一个 run，归并后仍保持 NULL-first ASC 和相等 key 的输入稳定次序；
     * close 必须删除本语句创建的所有 run 与独占目录。
     */
    @Test
    void mergesPartitionedRunsWithStableNullOrderingAndCleansFiles()
            throws IOException {
        TestRowsNode source = rows(List.of(
                row(2, 21), rowNull(9), row(1, 10),
                row(2, 22), row(3, 30)));
        PlanNode root = new SortNode(
                source, List.of(new PhysicalSortKey(0, IndexOrder.ASC)),
                PhysicalSortStrategy.PARTITIONED_HEAP_MERGE,
                Optional.empty(), new SortExecutionConfig(directory, 2));

        root.open(context());
        List<String> actual = new ArrayList<>();
        while (root.advance()) {
            SqlRowView row = root.current();
            String key = row.isNullAt(0) ? "NULL"
                    : ((SqlValue.IntegerValue) row.valueAt(0))
                    .value().toString();
            String identity = ((SqlValue.IntegerValue) row.valueAt(1))
                    .value().toString();
            actual.add(key + ":" + identity);
        }
        root.close();

        assertEquals(
                List.of("NULL:9", "1:10", "2:21", "2:22", "3:30"),
                actual);
        try (var entries = Files.list(directory)) {
            assertFalse(entries.findAny().isPresent(),
                    "排序关闭后不能遗留 statement 目录或 run 文件");
        }
    }

    /**
     * run 数超过 fan-in 时必须逐层二路归并；最终结果仍按用户 key 与输入 sequence 稳定，
     * 且中间层输入只在输出完整关闭后删除。
     */
    @Test
    void performsBoundedMultiRoundMerge() throws IOException {
        TestRowsNode source = rows(List.of(
                row(7, 70), row(1, 10), row(6, 60),
                row(2, 20), row(5, 50), row(3, 30),
                row(4, 40)));
        PlanNode root = new SortNode(
                source,
                List.of(new PhysicalSortKey(
                        0, IndexOrder.ASC)),
                PhysicalSortStrategy.PARTITIONED_HEAP_MERGE,
                Optional.empty(),
                new SortExecutionConfig(
                        directory, 1, 1024,
                        1024 * 1024, 2));

        assertEquals(
                List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L),
                pullFirstColumn(root));
        try (var entries = Files.list(directory)) {
            assertFalse(entries.findAny().isPresent());
        }
    }

    /**
     * optimizer 的宽度估算可能低于运行期值；Top-N 在发布任何行前超过内存预算时，
     * 必须把当前最佳集合和后续输入转交外部排序，不能重扫或丢失候选。
     */
    @Test
    void degradesTopNToExternalMergeWithoutRescan()
            throws IOException {
        TestRowsNode source = rows(List.of(
                row(5, 50), row(1, 10), row(4, 40),
                row(2, 20), row(3, 30)));
        PhysicalLimit limit = new PhysicalLimit(1, 2);
        PlanNode root = new LimitNode(new SortNode(
                source,
                List.of(new PhysicalSortKey(
                        0, IndexOrder.ASC)),
                PhysicalSortStrategy.TOP_N_HEAP,
                Optional.of(limit),
                new SortExecutionConfig(
                        directory, 2, 300,
                        1024 * 1024, 2)), limit);

        assertEquals(List.of(2L, 3L),
                pullFirstColumn(root));
        assertEquals(1, source.openCalls);
        assertEquals(1, source.closeCalls);
        try (var entries = Files.list(directory)) {
            assertFalse(entries.findAny().isPresent());
        }
    }

    /**
     * 临时文件峰值超过配置时 open 必须失败并由模板清理全部完成/半成品 run，
     * 不能返回部分排序结果或遗留 statement 目录。
     */
    @Test
    void rejectsTemporaryBudgetOverflowAndCleansRuns()
            throws IOException {
        ArrayList<List<SqlValue>> values =
                new ArrayList<>();
        for (int value = 100; value >= 1; value--) {
            values.add(row(value, value));
        }
        PlanNode root = new SortNode(
                rows(values),
                List.of(new PhysicalSortKey(
                        0, IndexOrder.ASC)),
                PhysicalSortStrategy.PARTITIONED_HEAP_MERGE,
                Optional.empty(),
                new SortExecutionConfig(
                        directory, 1, 1024,
                        1024, 2));

        assertThrows(SqlExecutionException.class,
                () -> root.open(context()));
        try (var entries = Files.list(directory)) {
            assertFalse(entries.findAny().isPresent());
        }
    }

    /**
     * 实例启动清理只消费受控 statement/run 命名空间，模拟崩溃遗留后应恢复为空根。
     */
    @Test
    void cleansControlledStaleStatementRuns()
            throws IOException {
        Path stale = directory.resolve(
                "statement-crashed");
        Files.createDirectories(stale);
        Files.write(stale.resolve("run-7.bin"),
                new byte[]{1, 2, 3});
        SortExecutionConfig config =
                new SortExecutionConfig(directory, 2);

        config.cleanupStaleStatements();

        try (var entries = Files.list(directory)) {
            assertFalse(entries.findAny().isPresent());
        }
    }

    /** 临时根被普通文件占用时启动清理必须 fail-closed，不能把路径错误降级为无遗留文件。 */
    @Test
    void rejectsTemporaryRootThatIsNotDirectory()
            throws IOException {
        Path invalidRoot = directory.resolve("sort-root");
        Files.write(invalidRoot, new byte[]{1});
        SortExecutionConfig config =
                new SortExecutionConfig(invalidRoot, 2);

        assertThrows(SqlExecutionException.class,
                config::cleanupStaleStatements);
    }

    /**
     * LIMIT 0 在 open 阶段不打开 child，避免无结果语句仍创建 cursor、ReadView 或锁。
     */
    @Test
    void zeroLimitDoesNotOpenChild() {
        TestRowsNode source = rows(List.of(row(1, 10)));
        PlanNode root = new LimitNode(source, new PhysicalLimit(0, 0));

        assertEquals(List.of(), pullFirstColumn(root));
        assertEquals(0, source.openCalls);
        assertEquals(0, source.closeCalls);
    }

    private static List<Long> pullFirstColumn(PlanNode root) {
        root.open(context());
        ArrayList<Long> result = new ArrayList<>();
        while (root.advance()) {
            result.add(((SqlValue.IntegerValue)
                    root.current().valueAt(0)).value().longValueExact());
        }
        root.close();
        return List.copyOf(result);
    }

    private static TestRowsNode rows(List<List<SqlValue>> values) {
        List<ResultColumn> columns = List.of(
                new ResultColumn("sort_key",
                        ColumnTypeDefinition.bigint(false, true)),
                new ResultColumn("identity",
                        ColumnTypeDefinition.bigint(false, false)));
        return new TestRowsNode(values.stream()
                .map(row -> (SqlRowView) new MaterializedSqlRowView(
                        row, columns))
                .toList(), columns);
    }

    private static List<SqlValue> row(long key, long identity) {
        return List.of(integer(key), integer(identity));
    }

    private static List<SqlValue> rowNull(long identity) {
        return List.of(SqlValue.NullValue.INSTANCE, integer(identity));
    }

    private static SqlValue.IntegerValue integer(long value) {
        return new SqlValue.IntegerValue(BigInteger.valueOf(value));
    }

    private static ExecutionContext context() {
        return new ExecutionContext(
                new TestHandle(),
                SqlStatementDeadline.after(Duration.ofSeconds(5)));
    }

    /** 测试输入节点只发布预物化行，并记录 child 是否被 LIMIT 正确打开和关闭。 */
    private static final class TestRowsNode extends AbstractPlanNode {
        private final List<SqlRowView> rows;
        private final List<ResultColumn> columns;
        private int position;
        private int openCalls;
        private int closeCalls;

        private TestRowsNode(
                List<SqlRowView> rows, List<ResultColumn> columns) {
            this.rows = List.copyOf(rows);
            this.columns = List.copyOf(columns);
        }

        @Override
        protected void openNode(ExecutionContext context) {
            openCalls++;
        }

        @Override
        protected SqlRowView advanceNode() {
            return position < rows.size() ? rows.get(position++) : null;
        }

        @Override
        protected void closeNode() {
            closeCalls++;
        }

        @Override
        public List<ResultColumn> columns() {
            return columns;
        }
    }

    private static final class TestHandle implements SqlTransactionHandle {
    }
}
