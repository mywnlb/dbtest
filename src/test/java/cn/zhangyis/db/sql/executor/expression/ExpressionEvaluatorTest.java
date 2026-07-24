package cn.zhangyis.db.sql.executor.expression;

import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.expression.*;
import cn.zhangyis.db.sql.parser.SourcePosition;
import cn.zhangyis.db.sql.type.SqlBoolean;
import cn.zhangyis.db.sql.type.SqlValue;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 验证 Executor 唯一表达式解释器的三值、短路与惰性取值边界。 */
class ExpressionEvaluatorTest {
    private static final SourcePosition POSITION =
            new SourcePosition(0, 1, 1);
    private static final ColumnTypeDefinition TYPE =
            ColumnTypeDefinition.bigint(false, true);
    private final ExpressionEvaluator evaluator =
            new ExpressionEvaluator();

    /** FALSE AND 与 TRUE OR 必须跳过右侧 comparison，避免无意义的 LOB/value 读取。 */
    @Test
    void shortCircuitsConjunctionAndDisjunction() {
        RecordingRow row = new RecordingRow(false, 0);
        BoundComparison comparison = comparison(integer(1));

        assertEquals(SqlBoolean.FALSE, evaluator.evaluate(
                new BoundConjunction(List.of(
                        truth(SqlBoolean.FALSE), comparison)), row));
        assertEquals(SqlBoolean.TRUE, evaluator.evaluate(
                new BoundDisjunction(List.of(
                        truth(SqlBoolean.TRUE), comparison)), row));

        assertEquals(0, row.nullChecks);
        assertEquals(0, row.comparisons);
        assertEquals(0, row.valueReads);
    }

    /** SQL NULL comparison 保持 UNKNOWN，WHERE 不能把它误当成匹配。 */
    @Test
    void preservesUnknownForNullComparison() {
        RecordingRow row = new RecordingRow(true, 0);

        assertEquals(SqlBoolean.UNKNOWN,
                evaluator.evaluate(comparison(integer(1)), row));
        assertFalse(evaluator.matches(comparison(integer(1)), row));
        assertEquals(2, row.nullChecks);
        assertEquals(0, row.comparisons);
    }

    /** IS NULL 只读取 null bitmap 语义，不请求列 payload。 */
    @Test
    void evaluatesNullTestWithoutHydratingValue() {
        RecordingRow row = new RecordingRow(false, 0);
        BoundNullTest test = new BoundNullTest(
                column(), BoundNullTestOperator.IS_NOT_NULL, POSITION);

        assertEquals(SqlBoolean.TRUE, evaluator.evaluate(test, row));
        assertEquals(1, row.nullChecks);
        assertEquals(0, row.comparisons);
        assertEquals(0, row.valueReads);
    }

    private static BoundColumnReference column() {
        return new BoundColumnReference(1, 0, TYPE, POSITION);
    }

    private static BoundComparison comparison(SqlValue value) {
        return new BoundComparison(
                column(), BoundComparisonOperator.EQUAL,
                new BoundLiteral(value, TYPE, POSITION));
    }

    private static BoundTruthLiteral truth(SqlBoolean value) {
        return new BoundTruthLiteral(value, POSITION);
    }

    private static SqlValue.IntegerValue integer(long value) {
        return new SqlValue.IntegerValue(
                BigInteger.valueOf(value));
    }

    /** 记录 evaluator 触发了哪一种 row-view 读取，模拟 projection/LOB 的惰性边界。 */
    private static final class RecordingRow implements SqlRowView {
        private final boolean nullValue;
        private final int comparison;
        private int nullChecks;
        private int comparisons;
        private int valueReads;

        private RecordingRow(
                boolean nullValue, int comparison) {
            this.nullValue = nullValue;
            this.comparison = comparison;
        }

        @Override
        public int width() {
            return 1;
        }

        @Override
        public SqlValue valueAt(int ordinal) {
            valueReads++;
            return integer(1);
        }

        @Override
        public boolean isNullAt(int ordinal) {
            nullChecks++;
            return nullValue;
        }

        @Override
        public int compareLiteral(
                int ordinal, SqlValue literal) {
            comparisons++;
            return comparison;
        }
    }
}
