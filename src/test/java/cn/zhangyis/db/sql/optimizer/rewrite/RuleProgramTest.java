package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.expression.BoundNegation;
import cn.zhangyis.db.sql.expression.BoundNullTest;
import cn.zhangyis.db.sql.expression.BoundNullTestOperator;
import cn.zhangyis.db.sql.expression.BoundTruthLiteral;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;
import cn.zhangyis.db.sql.optimizer.logical.LogicalFilter;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;
import cn.zhangyis.db.sql.optimizer.logical.LogicalProject;
import cn.zhangyis.db.sql.optimizer.logical.LogicalTableScan;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.parser.SourcePosition;
import cn.zhangyis.db.sql.type.SqlBoolean;
import cn.zhangyis.db.sql.type.SqlValue;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证规则程序的确定顺序、固定点终止与 SQL 三值等价改写。
 */
class RuleProgramTest {

    /** 默认规则展平 AND，并把普通 comparison 与 NULL 折叠为 UNKNOWN。 */
    @Test
    void rewritesNestedConjunctionToFixedPoint() {
        TableDefinition table = table();
        BoundExpression equality = equality(table, SqlValue.NullValue.INSTANCE);
        BoundExpression nested = new BoundConjunction(List.of(
                new BoundTruthLiteral(SqlBoolean.TRUE, position()),
                new BoundConjunction(List.of(
                        equality, new BoundTruthLiteral(SqlBoolean.TRUE, position())))));
        LogicalPlan rewritten = RuleProgram.standard().rewrite(plan(table, nested));

        LogicalProject project = assertInstanceOf(
                LogicalProject.class, rewritten.root());
        LogicalFilter filter = assertInstanceOf(
                LogicalFilter.class, project.input());
        BoundTruthLiteral truth = assertInstanceOf(
                BoundTruthLiteral.class, filter.predicates().condition());
        assertEquals(SqlBoolean.UNKNOWN, truth.value());
        assertSame(rewritten, RuleProgram.standard().rewrite(rewritten),
                "固定点输入不得反复分配等价逻辑计划");
    }

    /** changed=true 却返回等价计划属于规则协议损坏，必须在发布物理计划前失败。 */
    @Test
    void rejectsFalseChangedResult() {
        OptimizerRule invalid = new OptimizerRule() {
            @Override
            public String name() {
                return "invalid";
            }

            @Override
            public RuleResult apply(LogicalPlan plan) {
                return RuleResult.transformed(plan);
            }
        };
        RuleProgram program = new RuleProgram(List.of(invalid), 2);
        LogicalPlan plan = plan(table(), equality(table(),
                new SqlValue.IntegerValue(BigInteger.ONE)));

        assertThrows(SqlOptimizationException.class, () -> program.rewrite(plan));
    }

    /** 两条规则形成结构循环时必须在有界轮次内 fail-closed。 */
    @Test
    void rejectsRewriteCycle() {
        OptimizerRule toTrue = replacingTruthRule(
                "to-true", SqlBoolean.FALSE, SqlBoolean.TRUE);
        OptimizerRule toFalse = replacingTruthRule(
                "to-false", SqlBoolean.TRUE, SqlBoolean.FALSE);
        LogicalPlan input = plan(table(),
                new BoundTruthLiteral(SqlBoolean.FALSE, position()));

        assertThrows(SqlOptimizationException.class,
                () -> new RuleProgram(List.of(toTrue, toFalse), 4).rewrite(input));
    }

    /** literal-column comparison 必须交换为 column-literal，并同步反转范围方向。 */
    @Test
    void canonicalizesLiteralColumnComparison() {
        TableDefinition table = table();
        ColumnDefinition column = table.columns().getFirst();
        BoundLiteral literal = new BoundLiteral(
                new SqlValue.IntegerValue(BigInteger.ONE),
                column.type(), position());
        BoundColumnReference reference = new BoundColumnReference(
                column.columnId(), column.ordinal(),
                column.type(), position());

        LogicalProject project = assertInstanceOf(
                LogicalProject.class,
                RuleProgram.standard().rewrite(plan(
                        table, new BoundComparison(
                                literal,
                                BoundComparisonOperator.LESS_THAN,
                                reference))).root());
        BoundComparison comparison = assertInstanceOf(
                BoundComparison.class,
                assertInstanceOf(
                        LogicalFilter.class,
                        project.input()).predicates().condition());
        assertSame(reference, comparison.left());
        assertSame(literal, comparison.right());
        assertEquals(
                BoundComparisonOperator.GREATER_THAN,
                comparison.operator());
    }

    /**
     * 默认规则必须以 bottom-up 方式覆盖 OR、NOT 与 null-test，并在固定点后保留
     * SQL 三值等价；任何新节点都不能成为跳过子树改写的屏障。
     */
    @Test
    void rewritesBooleanTreeBottomUpAndFoldsTruth() {
        TableDefinition table = table();
        ColumnDefinition column = table.columns().getFirst();
        BoundLiteral one = new BoundLiteral(
                new SqlValue.IntegerValue(BigInteger.ONE),
                column.type(), position());
        BoundExpression condition = new BoundConjunction(List.of(
                new BoundDisjunction(List.of(
                        new BoundTruthLiteral(SqlBoolean.FALSE, position()),
                        new BoundDisjunction(List.of(
                                new BoundTruthLiteral(SqlBoolean.FALSE, position()),
                                new BoundTruthLiteral(SqlBoolean.TRUE, position()))))),
                new BoundNegation(
                        new BoundTruthLiteral(SqlBoolean.FALSE, position()),
                        position()),
                new BoundNullTest(
                        one, BoundNullTestOperator.IS_NOT_NULL, position())));

        LogicalProject project = assertInstanceOf(
                LogicalProject.class,
                RuleProgram.standard().rewrite(plan(table, condition)).root());
        BoundTruthLiteral truth = assertInstanceOf(
                BoundTruthLiteral.class,
                assertInstanceOf(
                        LogicalFilter.class,
                        project.input()).predicates().condition());
        assertEquals(SqlBoolean.TRUE, truth.value());
    }

    /** literal-column comparison 位于 NOT/OR 内部时仍必须规范化，不能只处理最外层 AND。 */
    @Test
    void canonicalizesComparisonInsideNegationAndDisjunction() {
        TableDefinition table = table();
        ColumnDefinition column = table.columns().getFirst();
        BoundLiteral literal = new BoundLiteral(
                new SqlValue.IntegerValue(BigInteger.ONE),
                column.type(), position());
        BoundColumnReference reference = new BoundColumnReference(
                column.columnId(), column.ordinal(),
                column.type(), position());
        BoundExpression condition = new BoundDisjunction(List.of(
                new BoundTruthLiteral(SqlBoolean.FALSE, position()),
                new BoundNegation(new BoundComparison(
                        literal, BoundComparisonOperator.LESS_THAN, reference),
                        position())));

        LogicalProject project = assertInstanceOf(
                LogicalProject.class,
                RuleProgram.standard().rewrite(plan(table, condition)).root());
        BoundNegation negation = assertInstanceOf(
                BoundNegation.class,
                assertInstanceOf(
                        LogicalFilter.class,
                        project.input()).predicates().condition());
        BoundComparison comparison = assertInstanceOf(
                BoundComparison.class, negation.operand());
        assertSame(reference, comparison.left());
        assertEquals(BoundComparisonOperator.GREATER_THAN,
                comparison.operator());
    }

    private static OptimizerRule replacingTruthRule(
            String name, SqlBoolean source, SqlBoolean target) {
        return new OptimizerRule() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public RuleResult apply(LogicalPlan plan) {
                LogicalProject project = (LogicalProject) plan.root();
                LogicalFilter filter = (LogicalFilter) project.input();
                if (filter.predicates().condition()
                        instanceof BoundTruthLiteral truth
                        && truth.value() == source) {
                    return RuleResult.transformed(new LogicalPlan(
                            new LogicalProject(new LogicalFilter(
                                    filter.input(), PredicateSet.of(
                                    new BoundTruthLiteral(target, truth.position()))),
                                    project.projectionOrdinals())));
                }
                return RuleResult.unchanged(plan);
            }
        };
    }

    private static LogicalPlan plan(TableDefinition table, BoundExpression condition) {
        return new LogicalPlan(new LogicalProject(
                new LogicalFilter(
                        new LogicalTableScan(table, SelectLockMode.CONSISTENT),
                        PredicateSet.of(condition)),
                List.of(0)));
    }

    private static BoundExpression equality(TableDefinition table, SqlValue value) {
        ColumnDefinition column = table.columns().getFirst();
        return new BoundComparison(
                new BoundColumnReference(
                        column.columnId(), column.ordinal(), column.type(), position()),
                BoundComparisonOperator.EQUAL,
                new BoundLiteral(value, column.type(), position()));
    }

    private static SourcePosition position() {
        return new SourcePosition(0, 1, 1);
    }

    private static TableDefinition table() {
        ColumnDefinition column = new ColumnDefinition(
                1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(
                IndexId.of(1), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        return new TableDefinition(
                TableId.of(1), SchemaId.of(1), ObjectName.of("items"),
                DictionaryVersion.of(1), TableState.ACTIVE,
                List.of(column), List.of(primary));
    }
}
