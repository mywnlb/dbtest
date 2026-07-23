package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.sql.parser.SourcePosition;
import cn.zhangyis.db.sql.type.SqlBoolean;
import cn.zhangyis.db.sql.type.SqlValue;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 BoundExpression 的 exact-type、源位置与 SQL 三值逻辑不变量。
 */
class BoundExpressionTest {

    /** AND 必须遵守 SQL 三值真值表，WHERE 只接受 TRUE。 */
    @Test
    void evaluatesSqlBooleanAndTruthTable() {
        assertEquals(SqlBoolean.TRUE, SqlBoolean.TRUE.and(SqlBoolean.TRUE));
        assertEquals(SqlBoolean.FALSE, SqlBoolean.TRUE.and(SqlBoolean.FALSE));
        assertEquals(SqlBoolean.UNKNOWN, SqlBoolean.TRUE.and(SqlBoolean.UNKNOWN));
        assertEquals(SqlBoolean.FALSE, SqlBoolean.UNKNOWN.and(SqlBoolean.FALSE));
        assertEquals(SqlBoolean.UNKNOWN, SqlBoolean.UNKNOWN.and(SqlBoolean.UNKNOWN));
        assertEquals(SqlBoolean.TRUE, SqlBoolean.TRUE.or(SqlBoolean.UNKNOWN));
        assertEquals(SqlBoolean.UNKNOWN, SqlBoolean.FALSE.or(SqlBoolean.UNKNOWN));
        assertEquals(SqlBoolean.FALSE, SqlBoolean.FALSE.or(SqlBoolean.FALSE));
        assertEquals(SqlBoolean.FALSE, SqlBoolean.TRUE.not());
        assertEquals(SqlBoolean.TRUE, SqlBoolean.FALSE.not());
        assertEquals(SqlBoolean.UNKNOWN, SqlBoolean.UNKNOWN.not());
        assertTrue(SqlBoolean.TRUE.matchesWhere());
        assertFalse(SqlBoolean.FALSE.matchesWhere());
        assertFalse(SqlBoolean.UNKNOWN.matchesWhere());
    }

    /** comparison 必须绑定相同 exact scalar type，并把 nullable 传播到 boolean 结果。 */
    @Test
    void validatesComparisonTypesAndNullability() {
        ColumnTypeDefinition bigint = ColumnTypeDefinition.bigint(false, false);
        SourcePosition position = new SourcePosition(7, 1, 8);
        BoundColumnReference column =
                new BoundColumnReference(11, 0, bigint, position);
        BoundLiteral value = new BoundLiteral(
                new SqlValue.IntegerValue(BigInteger.ONE), bigint, position);
        BoundComparison comparison =
                new BoundComparison(column, BoundComparisonOperator.EQUAL, value);

        assertEquals(position, comparison.position());
        assertFalse(comparison.type().nullable());

        BoundLiteral sqlNull =
                new BoundLiteral(SqlValue.NullValue.INSTANCE, bigint, position);
        assertTrue(new BoundComparison(
                column, BoundComparisonOperator.EQUAL, sqlNull).type().nullable());

        assertThrows(DatabaseValidationException.class, () -> new BoundComparison(
                column, BoundComparisonOperator.EQUAL,
                new BoundLiteral(new SqlValue.IntegerValue(BigInteger.ONE),
                        ColumnTypeDefinition.integer(false, false), position)));
    }

    /** conjunction 只接受 boolean operand，且保留最左表达式的源起始位置。 */
    @Test
    void rejectsScalarConjunctionOperand() {
        ColumnTypeDefinition bigint = ColumnTypeDefinition.bigint(false, false);
        SourcePosition position = new SourcePosition(0, 1, 1);
        BoundColumnReference column =
                new BoundColumnReference(1, 0, bigint, position);

        assertThrows(DatabaseValidationException.class,
                () -> new BoundConjunction(List.of(
                        column, new BoundTruthLiteral(SqlBoolean.TRUE, position))));
    }

    /**
     * OR/NOT 传播 boolean nullable；IS NULL/IS NOT NULL 无论列是否可空都只返回
     * TRUE/FALSE，并拒绝把 boolean expression 当作 scalar operand。
     */
    @Test
    void validatesBooleanCompositionAndNullTestTypes() {
        ColumnTypeDefinition nullableBigint =
                ColumnTypeDefinition.bigint(false, true);
        SourcePosition position = new SourcePosition(4, 1, 5);
        BoundColumnReference column =
                new BoundColumnReference(1, 0, nullableBigint, position);
        BoundLiteral value = new BoundLiteral(
                new SqlValue.IntegerValue(BigInteger.ONE),
                nullableBigint, position);
        BoundComparison comparison =
                new BoundComparison(column, BoundComparisonOperator.EQUAL, value);

        BoundDisjunction disjunction = new BoundDisjunction(List.of(
                comparison,
                new BoundTruthLiteral(SqlBoolean.FALSE, position)));
        assertTrue(disjunction.type().nullable());
        assertTrue(new BoundNegation(disjunction, position).type().nullable());

        BoundNullTest isNull = new BoundNullTest(
                column, BoundNullTestOperator.IS_NULL, position);
        assertFalse(isNull.type().nullable());
        assertEquals(position, isNull.position());
        assertInstanceOf(BoundExpressionType.Scalar.class,
                isNull.operand().type());
        assertThrows(DatabaseValidationException.class, () -> new BoundNullTest(
                disjunction, BoundNullTestOperator.IS_NOT_NULL, position));
    }
}
