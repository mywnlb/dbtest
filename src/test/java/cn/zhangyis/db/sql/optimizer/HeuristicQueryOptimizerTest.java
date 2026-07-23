package cn.zhangyis.db.sql.optimizer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
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
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.BoundSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundNullTestOperator;
import cn.zhangyis.db.sql.optimizer.logical.BoundToLogicalConverter;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointSelect;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeSelect;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSecondaryRangeSelect;
import cn.zhangyis.db.sql.optimizer.physical.PointAccessKind;
import cn.zhangyis.db.sql.optimizer.physical.RangeEndpoint;
import cn.zhangyis.db.sql.type.SqlValue;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.and;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.comparison;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.equal;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.indexEqualityPredicates;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.nullTest;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.or;
import static cn.zhangyis.db.sql.SqlExpressionTestFixture.predicates;

/**
 * 验证 M1 heuristic optimizer 独占访问路径选择，并冻结与旧 Binder 相同的确定性优先级。
 */
class HeuristicQueryOptimizerTest {
    private final BoundToLogicalConverter converter = new BoundToLogicalConverter();
    private final QueryOptimizer optimizer = new HeuristicQueryOptimizer();

    /** 完整聚簇键和完整唯一二级键必须产生 point plan，key value 按 index part 重排。 */
    @Test
    void choosesPrimaryThenUniqueSecondaryPointAccess() {
        TableDefinition table = table();
        BoundSelect primary = new BoundSelect(table, List.of(2, 0), and(
                predicate(table, 1, 2), predicate(table, 0, 7)),
                SelectLockMode.CONSISTENT);
        PhysicalPointSelect primaryPlan = assertInstanceOf(PhysicalPointSelect.class,
                optimizer.optimize(converter.convert(primary)));
        assertEquals(PointAccessKind.CLUSTERED_PRIMARY, primaryPlan.accessKind());
        assertEquals(3, primaryPlan.accessIndexId());
        assertEquals(List.of(integer(7), integer(2)), primaryPlan.keyValues());

        BoundSelect unique = new BoundSelect(table, List.of(0),
                equal(table, 2, new SqlValue.StringValue("memo")),
                SelectLockMode.CONSISTENT);
        PhysicalPointSelect uniquePlan = assertInstanceOf(PhysicalPointSelect.class,
                optimizer.optimize(converter.convert(unique)));
        assertEquals(PointAccessKind.UNIQUE_SECONDARY, uniquePlan.accessKind());
        assertEquals(4, uniquePlan.accessIndexId());
    }

    /** comparison range 使用最长连续前缀；没有首列约束时回退聚簇全扫。 */
    @Test
    void choosesLongestRangePrefixAndClusteredFallback() {
        TableDefinition table = table();
        BoundSelect range = new BoundSelect(table, List.of(0), and(
                equal(table, 3, new SqlValue.StringValue("open")),
                comparison(table, 1,
                        BoundComparisonOperator.GREATER_THAN_OR_EQUAL,
                        integer(2))),
                SelectLockMode.CONSISTENT);
        PhysicalRangeSelect rangePlan = assertInstanceOf(PhysicalRangeSelect.class,
                optimizer.optimize(converter.convert(range)));
        assertEquals(6, rangePlan.accessIndexId());
        assertTrue(rangePlan.indexRange().upper().isPresent(),
                "第二 key part 为 DESC，SQL lower bound 必须翻转为物理 upper bound");

        BoundSelect scan = new BoundSelect(table, List.of(0),
                comparison(table, 1, BoundComparisonOperator.GREATER_THAN,
                        integer(2)),
                SelectLockMode.CONSISTENT);
        PhysicalRangeSelect scanPlan = assertInstanceOf(PhysicalRangeSelect.class,
                optimizer.optimize(converter.convert(scan)));
        assertEquals(3, scanPlan.accessIndexId());
        assertTrue(scanPlan.indexRange().lower().isEmpty());
        assertTrue(scanPlan.indexRange().upper().isEmpty());
    }

    /** 普通二级等值保留多行 prefix-range；SQL NULL conjunction 生成 empty range 而不访问 point 路径。 */
    @Test
    void choosesNonUniquePrefixAndProvesNullPredicateEmpty() {
        TableDefinition table = table();
        BoundSelect lockingPrimary = new BoundSelect(
                table, List.of(0), and(
                predicate(table, 0, 7), predicate(table, 1, 2)),
                SelectLockMode.FOR_UPDATE);
        PhysicalRangeSelect lockingPlan = assertInstanceOf(PhysicalRangeSelect.class,
                optimizer.optimize(converter.convert(lockingPrimary)));
        assertEquals(SelectLockMode.FOR_UPDATE, lockingPlan.lockMode(),
                "locking point 必须保留 current-read intent，不能错误降为一致性 point plan");

        BoundSelect secondary = new BoundSelect(table, List.of(0),
                equal(table, 3, new SqlValue.StringValue("open")),
                SelectLockMode.FOR_SHARE);
        PhysicalSecondaryRangeSelect secondaryPlan =
                assertInstanceOf(PhysicalSecondaryRangeSelect.class,
                        optimizer.optimize(converter.convert(secondary)));
        assertEquals(5, secondaryPlan.accessIndexId());
        assertEquals(SelectLockMode.FOR_SHARE, secondaryPlan.lockMode());

        BoundSelect nullPredicate = new BoundSelect(table, List.of(0),
                equal(table, 0, SqlValue.NullValue.INSTANCE),
                SelectLockMode.CONSISTENT);
        PhysicalRangeSelect empty = assertInstanceOf(PhysicalRangeSelect.class,
                optimizer.optimize(converter.convert(nullPredicate)));
        assertTrue(empty.empty());
        assertEquals(3, empty.accessIndexId());
        assertTrue(empty.indexRange().lower().isEmpty());
        assertTrue(empty.indexRange().upper().isEmpty());
        assertThrows(DatabaseValidationException.class,
                () -> new RangeEndpoint(List.of(SqlValue.NullValue.INSTANCE), true),
                "SQL NULL 只能形成 empty 证明，不能下沉为 B+Tree endpoint");
    }

    /** DML 只有完整聚簇等值才降为 point；额外 residual 必须保留范围语句原子路径。 */
    @Test
    void choosesPointOnlyForExactPrimaryDmlPredicate() {
        TableDefinition table = table();
        BoundExpression primary = and(
                predicate(table, 0, 7), predicate(table, 1, 2));
        BoundUpdate update = new BoundUpdate(table, List.of(2),
                List.of(new SqlValue.StringValue("changed")), primary);
        PhysicalPointUpdate updatePlan = assertInstanceOf(PhysicalPointUpdate.class,
                optimizer.optimize(converter.convert(update)));
        assertEquals(List.of(integer(7), integer(2)), updatePlan.primaryKeyValues());

        PhysicalPointDelete deletePlan = assertInstanceOf(PhysicalPointDelete.class,
                optimizer.optimize(converter.convert(new BoundDelete(table, primary))));
        assertEquals(List.of(integer(7), integer(2)), deletePlan.primaryKeyValues());

        BoundExpression withResidual = and(
                predicate(table, 0, 7), predicate(table, 1, 2),
                equal(table, 3, new SqlValue.StringValue("open")));
        PhysicalRangeUpdate rangeUpdate = assertInstanceOf(PhysicalRangeUpdate.class,
                optimizer.optimize(converter.convert(new BoundUpdate(
                        table, List.of(2), List.of(new SqlValue.StringValue("changed")),
                        withResidual))));
        PhysicalRangeDelete rangeDelete = assertInstanceOf(PhysicalRangeDelete.class,
                optimizer.optimize(converter.convert(new BoundDelete(table, withResidual))));
        assertEquals(3, rangeUpdate.accessIndexId(),
                "额外 residual 不能被 point DML 丢弃；同分范围必须按 stable index id 选择");
        assertEquals(PredicateSet.of(withResidual), rangeUpdate.predicates());
        assertEquals(PredicateSet.of(withResidual), rangeDelete.predicates());
    }

    /**
     * 一致性 SELECT 只要最外层正向 AND 完整证明唯一键，就可以 point 定位后执行
     * opaque residual；OR 根本身不能提供必要条件，必须退化为聚簇全扫。
     */
    @Test
    void usesPointForProvenKeyWithOpaqueResidualButNotForRootOr() {
        TableDefinition table = table();
        BoundExpression opaqueResidual = or(
                equal(table, 3, new SqlValue.StringValue("open")),
                equal(table, 2, new SqlValue.StringValue("memo")));
        BoundSelect point = new BoundSelect(
                table, List.of(0), and(
                predicate(table, 0, 7),
                predicate(table, 1, 2),
                opaqueResidual),
                SelectLockMode.CONSISTENT);

        PhysicalPointSelect pointPlan = assertInstanceOf(
                PhysicalPointSelect.class,
                optimizer.optimize(converter.convert(point)));
        assertEquals(3, pointPlan.accessIndexId());
        assertEquals(List.of(integer(7), integer(2)),
                pointPlan.keyValues());

        BoundSelect rootOr = new BoundSelect(
                table, List.of(0), new BoundDisjunction(List.of(
                and(predicate(table, 0, 7), predicate(table, 1, 2)),
                equal(table, 3, new SqlValue.StringValue("open")))),
                SelectLockMode.CONSISTENT);
        PhysicalRangeSelect scan = assertInstanceOf(
                PhysicalRangeSelect.class,
                optimizer.optimize(converter.convert(rootOr)));
        assertEquals(3, scan.accessIndexId());
        assertTrue(scan.indexRange().lower().isEmpty());
        assertTrue(scan.indexRange().upper().isEmpty());
    }

    /**
     * Point DML 不携带 residual，因此主键 equality 之外出现 null-test 等 opaque
     * 条件时必须走原子 range mutation，并完整保留最终真值。
     */
    @Test
    void keepsOpaqueResidualOnRangeDml() {
        TableDefinition table = table();
        BoundExpression condition = and(
                predicate(table, 0, 7),
                predicate(table, 1, 2),
                nullTest(table, 2, BoundNullTestOperator.IS_NULL));

        PhysicalRangeUpdate update = assertInstanceOf(
                PhysicalRangeUpdate.class,
                optimizer.optimize(converter.convert(new BoundUpdate(
                        table, List.of(2),
                        List.of(new SqlValue.StringValue("changed")),
                        condition))));
        PhysicalRangeDelete delete = assertInstanceOf(
                PhysicalRangeDelete.class,
                optimizer.optimize(converter.convert(
                        new BoundDelete(table, condition))));
        assertEquals(3, update.accessIndexId());
        assertEquals(PredicateSet.of(condition), update.predicates());
        assertEquals(PredicateSet.of(condition), delete.predicates());
    }

    /** 物理点查自身必须拒绝 LOB/JSON key，不能只依赖当前 heuristic optimizer 的前置判断。 */
    @Test
    void physicalPointPlanRejectsLobIndexFromAlternativeOptimizer() {
        TableDefinition table = lobIndexTable();
        List<SqlValue> key = List.of(
                new SqlValue.BytesValue(new byte[]{1}));
        assertThrows(DatabaseValidationException.class, () -> new PhysicalPointSelect(
                table, List.of(0), 8, PointAccessKind.UNIQUE_SECONDARY,
                key, indexEqualityPredicates(table, 8, key)));
    }

    /** 替代 optimizer 不能提交未由完整 residual 证明的 point key，否则会 under-scan 漏行。 */
    @Test
    void physicalPointPlanRejectsKeyNotProvenByResidual() {
        TableDefinition table = table();
        List<SqlValue> key = List.of(
                new SqlValue.StringValue("memo"));

        assertThrows(DatabaseValidationException.class,
                () -> new PhysicalPointSelect(
                        table, List.of(0), 4,
                        PointAccessKind.UNIQUE_SECONDARY, key,
                        predicates(equal(
                                table, 3,
                                new SqlValue.StringValue("open")))));
    }

    /** OR 分支中的 equality 不是查询的必要条件，不能用于证明 point key。 */
    @Test
    void physicalPointPlanRejectsKeyProvenOnlyInsideDisjunction() {
        TableDefinition table = table();
        List<SqlValue> key = List.of(integer(7), integer(2));

        assertThrows(DatabaseValidationException.class,
                () -> new PhysicalPointSelect(
                        table, List.of(0), 3,
                        PointAccessKind.CLUSTERED_PRIMARY, key,
                        PredicateSet.of(or(
                                and(predicate(table, 0, 7),
                                        predicate(table, 1, 2)),
                                equal(table, 3,
                                        new SqlValue.StringValue("open"))))));
    }

    private static BoundExpression predicate(
            TableDefinition table, int ordinal, long value) {
        return equal(table, ordinal, integer(value));
    }

    private static SqlValue.IntegerValue integer(long value) {
        return new SqlValue.IntegerValue(BigInteger.valueOf(value));
    }

    private static TableDefinition table() {
        List<ColumnDefinition> columns = List.of(
                new ColumnDefinition(1, ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false), 0),
                new ColumnDefinition(2, ObjectName.of("tenant"), ColumnTypeDefinition.integer(false, false), 1),
                new ColumnDefinition(3, ObjectName.of("note"), new ColumnTypeDefinition(
                        DictionaryTypeId.VARCHAR, false, true, 128, 0, 1, 1, List.of()), 2),
                new ColumnDefinition(4, ObjectName.of("status"), new ColumnTypeDefinition(
                        DictionaryTypeId.VARCHAR, false, false, 32, 0, 1, 1, List.of()), 3));
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0),
                        new IndexKeyPart(2, IndexOrder.ASC, 0)));
        IndexDefinition note = new IndexDefinition(IndexId.of(4), ObjectName.of("uq_note"), true, false,
                List.of(new IndexKeyPart(3, IndexOrder.ASC, 0)));
        IndexDefinition status = new IndexDefinition(IndexId.of(5), ObjectName.of("idx_status"), false, false,
                List.of(new IndexKeyPart(4, IndexOrder.ASC, 0)));
        IndexDefinition composite = new IndexDefinition(
                IndexId.of(6), ObjectName.of("idx_status_tenant"), false, false,
                List.of(new IndexKeyPart(4, IndexOrder.ASC, 0),
                        new IndexKeyPart(2, IndexOrder.DESC, 0)));
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"),
                DictionaryVersion.of(2), TableState.ACTIVE, columns,
                List.of(primary, note, status, composite));
    }

    private static TableDefinition lobIndexTable() {
        List<ColumnDefinition> columns = List.of(
                new ColumnDefinition(1, ObjectName.of("id"),
                        ColumnTypeDefinition.bigint(false, false), 0),
                new ColumnDefinition(2, ObjectName.of("payload"),
                        ColumnTypeDefinition.scalar(DictionaryTypeId.BLOB, false, false), 1));
        IndexDefinition primary = new IndexDefinition(
                IndexId.of(7), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        IndexDefinition lob = new IndexDefinition(
                IndexId.of(8), ObjectName.of("uq_payload"), true, false,
                List.of(new IndexKeyPart(2, IndexOrder.ASC, 0)));
        return new TableDefinition(
                TableId.of(3), SchemaId.of(1), ObjectName.of("lob_keys"),
                DictionaryVersion.of(2), TableState.ACTIVE, columns, List.of(primary, lob));
    }
}
