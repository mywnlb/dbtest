package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.expression.ExpressionEvaluator;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.parser.SourcePosition;
import cn.zhangyis.db.sql.type.SqlValue;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证普通/索引 probe 共用的 Nested Loop Join 运行语义与 child 资源边界。 */
class NestedLoopJoinNodeTest {
    private static final ColumnTypeDefinition KEY_TYPE =
            ColumnTypeDefinition.bigint(false, true);
    private static final List<ResultColumn> COLUMNS =
            List.of(new ResultColumn("id", KEY_TYPE));

    /**
     * outer NULL key 不得打开 inner；每个非 NULL outer row 创建一次 NEW inner，
     * ON 只发布 TRUE，且旧 joined view 在任一 child 前进后失效。
     */
    @Test
    void reopensInnerPerOuterRowSkipsNullAndInvalidatesOldView() {
        GenerationRowsNode outer = rows(
                integer(1), SqlValue.NullValue.INSTANCE,
                integer(2));
        AtomicInteger probes = new AtomicInteger();
        List<GenerationRowsNode> inners =
                new ArrayList<>();
        NestedLoopJoinNode join =
                new NestedLoopJoinNode(
                        outer,
                        key -> {
                            probes.incrementAndGet();
                            GenerationRowsNode inner =
                                    key.equals(integer(1))
                                            ? rows(integer(9), integer(1))
                                            : rows(integer(2));
                            inners.add(inner);
                            return inner;
                        },
                        0, joinPredicate(),
                        new ExpressionEvaluator(),
                        COLUMNS);

        join.open(context());
        assertTrue(join.advance());
        SqlRowView first = join.current();
        assertEquals(integer(1),
                first.valueAt(0));
        assertEquals(integer(1),
                first.valueAt(1));
        assertTrue(join.advance());
        assertEquals(integer(2),
                join.current().valueAt(0));
        assertThrows(AssertionError.class,
                () -> first.valueAt(0),
                "inner/outer 前进后旧 joined view 必须随 child generation 失效");
        assertFalse(join.advance());
        join.close();

        assertEquals(2, probes.get(),
                "NULL outer key 不创建无结果 inner probe");
        assertEquals(1, outer.openCalls);
        assertEquals(1, outer.closeCalls);
        assertTrue(inners.stream().allMatch(
                inner -> inner.openCalls == 1
                        && inner.closeCalls == 1));
    }

    private static PredicateSet joinPredicate() {
        SourcePosition position =
                new SourcePosition(0, 1, 1);
        return PredicateSet.of(new BoundComparison(
                new BoundColumnReference(
                        0, 1, 0, KEY_TYPE,
                        position),
                BoundComparisonOperator.EQUAL,
                new BoundColumnReference(
                        1, 2, 0, KEY_TYPE,
                        position)));
    }

    private static GenerationRowsNode rows(
            SqlValue... values) {
        return new GenerationRowsNode(
                java.util.Arrays.stream(values)
                        .map(List::of).toList(),
                COLUMNS);
    }

    private static SqlValue.IntegerValue integer(
            long value) {
        return new SqlValue.IntegerValue(
                BigInteger.valueOf(value));
    }

    private static ExecutionContext context() {
        return new ExecutionContext(
                new TestHandle(),
                SqlStatementDeadline.after(
                        Duration.ofSeconds(2)));
    }

    /**
     * 模拟 cursor generation 的测试节点；每次 advance 都使之前的 row view 失效。
     */
    private static final class GenerationRowsNode
            extends AbstractPlanNode {
        private final List<List<SqlValue>> rows;
        private final List<ResultColumn> columns;
        private int position = -1;
        private int generation;
        private int openCalls;
        private int closeCalls;

        private GenerationRowsNode(
                List<List<SqlValue>> rows,
                List<ResultColumn> columns) {
            this.rows = List.copyOf(rows);
            this.columns = List.copyOf(columns);
        }

        @Override
        protected void openNode(
                ExecutionContext context) {
            openCalls++;
        }

        @Override
        protected SqlRowView advanceNode() {
            generation++;
            position++;
            if (position >= rows.size()) {
                return null;
            }
            int expected = generation;
            List<SqlValue> row =
                    rows.get(position);
            return new SqlRowView() {
                private void valid() {
                    if (generation != expected) {
                        throw new AssertionError(
                                "stale row view");
                    }
                }

                @Override
                public int width() {
                    valid();
                    return row.size();
                }

                @Override
                public SqlValue valueAt(
                        int ordinal) {
                    valid();
                    return row.get(ordinal);
                }

                @Override
                public boolean isNullAt(
                        int ordinal) {
                    valid();
                    return row.get(ordinal)
                            instanceof SqlValue.NullValue;
                }

                @Override
                public int compareLiteral(
                        int ordinal,
                        SqlValue literal) {
                    valid();
                    BigInteger left =
                            ((SqlValue.IntegerValue)
                                    row.get(ordinal)).value();
                    BigInteger right =
                            ((SqlValue.IntegerValue)
                                    literal).value();
                    return left.compareTo(right);
                }
            };
        }

        @Override
        protected void closeNode() {
            generation++;
            closeCalls++;
        }

        @Override
        public List<ResultColumn> columns() {
            return columns;
        }
    }

    private static final class TestHandle
            implements SqlTransactionHandle {
    }
}
