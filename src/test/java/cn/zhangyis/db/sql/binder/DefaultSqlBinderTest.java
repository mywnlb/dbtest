package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.BoundSecondaryRangeSelect;
import cn.zhangyis.db.sql.binder.bound.PointAccessKind;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
import cn.zhangyis.db.sql.binder.bound.BoundDropIndex;
import cn.zhangyis.db.sql.binder.bound.BoundRangeDelete;
import cn.zhangyis.db.sql.binder.bound.BoundRangeSelect;
import cn.zhangyis.db.sql.binder.bound.BoundRangeUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundRowPredicateOperator;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.sql.parser.ast.CreateIndexStatementNode;
import cn.zhangyis.db.sql.parser.ast.DropIndexStatementNode;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.parser.DefaultSqlParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** 名称、列全集、投影和完整聚簇主键 shape 的 Binder TDD。 */
class DefaultSqlBinderTest {
    @TempDir Path directory;
    private final DefaultSqlParser parser = new DefaultSqlParser();
    private final DefaultSqlBinder binder = new DefaultSqlBinder(new SqlTypeCoercion());

    /** INSERT 输入列可换序/变更大小写，但 bound 值必须按 exact DD ordinal 排列。 */
    @Test
    void bindsCompleteInsertToCanonicalDictionaryOrder() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(200));
             StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundClusteredInsert bound = assertInstanceOf(BoundClusteredInsert.class, binder.bind(
                    parser.parse("INSERT INTO ORDERS (STATUS, NOTE, ID, TENANT) VALUES ('open', 'x', 7, 2)"),
                    context(statement, Optional.of(ObjectName.of("app")))));
            assertEquals(List.of(new SqlValue.IntegerValue(BigInteger.valueOf(7)),
                    new SqlValue.IntegerValue(BigInteger.valueOf(2)), new SqlValue.StringValue("x"),
                    new SqlValue.StringValue("open")),
                    bound.values());
            assertEquals("orders", bound.table().name().canonicalName());
            assertEquals(2, fixture.locks.snapshot().granted().size());
        }
    }

    /** 三段名、投影顺序和乱序谓词都规范化；key values 严格按复合主键 part 顺序。 */
    @Test
    void bindsThreePartPrimaryPointSelect() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(201));
             StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundPointSelect bound = assertInstanceOf(BoundPointSelect.class, binder.bind(
                    parser.parse("SELECT Note, ID FROM def.APP.ORDERS WHERE TENANT=2 AND ID=7"),
                    context(statement, Optional.empty())));
            assertEquals(List.of(2, 0), bound.projectionOrdinals());
            assertEquals(PointAccessKind.CLUSTERED_PRIMARY, bound.accessKind());
            assertEquals(3, bound.accessIndexId());
            assertEquals(List.of(new SqlValue.IntegerValue(BigInteger.valueOf(7)),
                    new SqlValue.IntegerValue(BigInteger.valueOf(2))), bound.keyValues());
        }
    }

    /** 完整 unique secondary 谓词可绑定为回表点查；Binder 只携带稳定 index id，不泄漏 storage metadata。 */
    @Test
    void bindsCompleteUniqueSecondaryPointSelect() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(204));
             StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundPointSelect bound = assertInstanceOf(BoundPointSelect.class, binder.bind(
                    parser.parse("SELECT ID, NOTE FROM orders WHERE NOTE='Hello'"),
                    context(statement, Optional.of(ObjectName.of("app")))));
            assertEquals(PointAccessKind.UNIQUE_SECONDARY, bound.accessKind());
            assertEquals(4, bound.accessIndexId());
            assertEquals(List.of(new SqlValue.StringValue("Hello")), bound.keyValues());
        }
    }

    /** non-unique logical key 等值必须绑定为多行 prefix range；locking clause 同时提升 metadata intent。 */
    @Test
    void bindsNonUniqueSecondaryRangeAndLockMode() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(207))) {
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundSecondaryRangeSelect bound = assertInstanceOf(BoundSecondaryRangeSelect.class, binder.bind(
                        parser.parse("SELECT id, status FROM orders WHERE status='open'"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(List.of(0, 3), bound.projectionOrdinals());
                assertEquals(5, bound.accessIndexId());
                assertEquals(List.of(new SqlValue.StringValue("open")), bound.logicalKeyValues());
                assertEquals(SelectLockMode.CONSISTENT, bound.lockMode());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundSecondaryRangeSelect bound = assertInstanceOf(BoundSecondaryRangeSelect.class, binder.bind(
                        parser.parse("SELECT * FROM orders WHERE status='open' FOR UPDATE"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(SelectLockMode.FOR_UPDATE, bound.lockMode());
                assertEquals(4, bound.projectionOrdinals().size());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundSecondaryRangeSelect bound = assertInstanceOf(BoundSecondaryRangeSelect.class, binder.bind(
                        parser.parse("SELECT id FROM orders WHERE status='open' FOR SHARE"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(SelectLockMode.FOR_SHARE, bound.lockMode());
            }
        }
    }

    /**
     * Binder 应选择最长连续复合前缀，并把 DESC part 的 SQL 下界翻转为物理上界；
     * 所有谓词仍保留为 residual，防止索引边界近似造成错误命中。
     */
    @Test
    void bindsCompositeComparisonRangeAndResidualPredicates() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(208));
             StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundRangeSelect bound = assertInstanceOf(BoundRangeSelect.class, binder.bind(
                    parser.parse("""
                            SELECT id, tenant FROM orders
                            WHERE status='open' AND tenant>=2 AND tenant<9
                            """),
                    context(statement, Optional.of(ObjectName.of("app")))));

            assertEquals(6, bound.accessIndexId());
            assertEquals(List.of(new SqlValue.StringValue("open"),
                            new SqlValue.IntegerValue(BigInteger.valueOf(9))),
                    bound.indexRange().lower().orElseThrow().keyValues());
            assertFalse(bound.indexRange().lower().orElseThrow().inclusive());
            assertEquals(List.of(new SqlValue.StringValue("open"),
                            new SqlValue.IntegerValue(BigInteger.valueOf(2))),
                    bound.indexRange().upper().orElseThrow().keyValues());
            assertTrue(bound.indexRange().upper().orElseThrow().inclusive());
            assertEquals(3, bound.predicates().size());
            assertEquals(BoundRowPredicateOperator.GREATER_THAN_OR_EQUAL,
                    bound.predicates().get(1).operator());
        }
    }

    /** 无首列可用索引时回退聚簇 full scan；数值矛盾范围直接发布 empty plan，不访问 storage。 */
    @Test
    void bindsClusteredFullScanAndDetectsEmptyIntersection() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(209))) {
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundRangeSelect fullScan = assertInstanceOf(BoundRangeSelect.class, binder.bind(
                        parser.parse("SELECT id FROM orders WHERE tenant>2"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(3, fullScan.accessIndexId());
                assertTrue(fullScan.indexRange().lower().isEmpty());
                assertTrue(fullScan.indexRange().upper().isEmpty());
                assertFalse(fullScan.empty());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundRangeSelect empty = assertInstanceOf(BoundRangeSelect.class, binder.bind(
                        parser.parse("SELECT id FROM orders WHERE id>10 AND id<=10"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertTrue(empty.empty());
            }
        }
    }

    /** 非 point UPDATE/DELETE 应冻结为 FOR_UPDATE range plan，赋值仍禁止触碰聚簇 key。 */
    @Test
    void bindsRangeUpdateAndDeleteWithoutLosingPointCompatibility() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(210))) {
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundRangeUpdate update = assertInstanceOf(BoundRangeUpdate.class, binder.bind(
                        parser.parse("UPDATE orders SET note='archived' WHERE status='old' AND tenant>=2"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(List.of(2), update.assignmentOrdinals());
                assertEquals(6, update.accessIndexId());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundRangeDelete delete = assertInstanceOf(BoundRangeDelete.class, binder.bind(
                        parser.parse("DELETE FROM orders WHERE tenant BETWEEN 2 AND 8"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(3, delete.accessIndexId());
                assertTrue(delete.indexRange().lower().isEmpty());
                assertTrue(delete.indexRange().upper().isEmpty());
            }
        }
    }

    /** 点 UPDATE/DELETE 只接受完整聚簇主键；赋值按 ordinal、主键按 index part 顺序冻结。 */
    @Test
    void bindsPrimaryPointUpdateAndDelete() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(205))) {
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundUpdate update = assertInstanceOf(BoundUpdate.class, binder.bind(
                        parser.parse("UPDATE orders SET status='closed', note='changed' WHERE tenant=2 AND id=7"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(List.of(2, 3), update.assignmentOrdinals());
                assertEquals(List.of(new SqlValue.StringValue("changed"),
                        new SqlValue.StringValue("closed")), update.assignmentValues());
                assertEquals(List.of(new SqlValue.IntegerValue(BigInteger.valueOf(7)),
                        new SqlValue.IntegerValue(BigInteger.valueOf(2))), update.primaryKeyValues());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundDelete delete = assertInstanceOf(BoundDelete.class, binder.bind(
                        parser.parse("DELETE FROM orders WHERE tenant=2 AND id=7"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(List.of(new SqlValue.IntegerValue(BigInteger.valueOf(7)),
                        new SqlValue.IntegerValue(BigInteger.valueOf(2))), delete.primaryKeyValues());
            }
        }
    }

    /** UPDATE/DELETE v1 禁止主键赋值、重复赋值和任何非完整聚簇主键定位。 */
    @Test
    void rejectsUnsafePointWriteShapesWithoutPublishingLeases() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(206))) {
            assertBindFails(transaction, "UPDATE orders SET id=8 WHERE id=7 AND tenant=2");
            assertBindFails(transaction, "UPDATE orders SET note='a', NOTE='b' WHERE id=7 AND tenant=2");
            assertDoesNotThrow(() -> bind(transaction, "UPDATE orders SET note='a' WHERE id=7"));
            assertDoesNotThrow(() -> bind(transaction,
                    "UPDATE orders SET note='a' WHERE id=7 AND tenant=2 AND note='old'"));
            assertDoesNotThrow(() -> bind(transaction, "UPDATE orders SET note='a' WHERE note='old'"));
            assertBindFails(transaction, "UPDATE orders SET note='a' WHERE id=7 AND id=8 AND tenant=2");
            assertDoesNotThrow(() -> bind(transaction, "DELETE FROM orders WHERE id=7"));
            assertDoesNotThrow(() -> bind(transaction, "DELETE FROM orders WHERE note='old'"));
            assertBindFails(transaction, "DELETE FROM prefix_key WHERE code='x'");
            assertBindFails(transaction, "DELETE FROM lob_key WHERE body='x'");
            assertFalse(fixture.locks.snapshot().granted().isEmpty());
        }
    }

    /** v1 不允许不完整/多余主键、prefix/Lob 主键、缺列 INSERT 或无物理 binding。 */
    @Test
    void rejectsUnsupportedShapesWithoutPublishingLeases() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(202))) {
            assertDoesNotThrow(() -> bind(transaction, "SELECT * FROM orders WHERE id=1"));
            assertDoesNotThrow(() -> bind(transaction,
                    "SELECT * FROM orders WHERE id=1 AND tenant=2 AND note='x'"));
            assertDoesNotThrow(() -> bind(transaction, "SELECT * FROM orders WHERE note='x' FOR UPDATE"));
            assertDoesNotThrow(() -> bind(transaction, "SELECT * FROM orders WHERE status='x' AND id=1"));
            assertBindFails(transaction, "INSERT INTO orders (id, tenant) VALUES (1, 2)");
            assertBindFails(transaction, "SELECT * FROM prefix_key WHERE code='x'");
            assertBindFails(transaction, "SELECT * FROM lob_key WHERE body='x'");
            assertBindFails(transaction, "SELECT * FROM unbound WHERE id=1");
            assertFalse(fixture.locks.snapshot().granted().isEmpty());
        }
    }

    /** 未限定表名必须有 current schema；不能偷偷选择默认 schema。 */
    @Test
    void rejectsUnqualifiedNameWithoutCurrentSchema() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(203));
             StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            assertThrows(SqlBindingException.class, () -> binder.bind(
                    parser.parse("SELECT * FROM orders WHERE id=1 AND tenant=2"),
                    context(statement, Optional.empty())));
            assertTrue(fixture.locks.snapshot().granted().isEmpty());
        }
    }

    /** DDL binder 不打开 DD lease，只统一 schema/name/order 并拒绝重复 key column。 */
    @Test
    void bindsCreateAndAlterIndexWithoutTransactionMetadataScope() {
        BoundCreateIndex create = binder.bindDdl(
                assertInstanceOf(CreateIndexStatementNode.class,
                        parser.parse("CREATE INDEX idx_status ON orders (status DESC)")),
                Optional.of(ObjectName.of("app")));
        BoundCreateIndex alter = binder.bindDdl(
                assertInstanceOf(CreateIndexStatementNode.class,
                        parser.parse("ALTER TABLE app.orders ADD INDEX idx_status (status DESC)")),
                Optional.empty());

        assertEquals(create.command(), alter.command());
        assertEquals(IndexOrder.DESC,
                create.command().index().keyParts().getFirst().order());
        assertThrows(SqlBindingException.class, () -> binder.bindDdl(
                assertInstanceOf(CreateIndexStatementNode.class,
                        parser.parse("CREATE INDEX bad ON app.orders (status, STATUS)")),
                Optional.empty()));
    }

    /** DROP 两种语法只绑定规范化 table/index name，不提前打开 DD lease 或触发 implicit commit。 */
    @Test
    void bindsDropAndAlterDropIndexWithoutTransactionMetadataScope() {
        BoundDropIndex drop = binder.bindDdl(
                assertInstanceOf(DropIndexStatementNode.class,
                        parser.parse("DROP INDEX idx_status ON orders")),
                Optional.of(ObjectName.of("app")));
        BoundDropIndex alter = binder.bindDdl(
                assertInstanceOf(DropIndexStatementNode.class,
                        parser.parse("ALTER TABLE app.orders DROP INDEX idx_status")),
                Optional.empty());

        assertEquals(drop.command(), alter.command());
        assertEquals(ObjectName.of("idx_status"), drop.command().indexName());
        assertThrows(SqlBindingException.class, () -> binder.bindDdl(
                assertInstanceOf(DropIndexStatementNode.class,
                        parser.parse("DROP INDEX idx_status ON orders")),
                Optional.empty()));
    }

    private void assertBindFails(TransactionMetadataScope transaction, String sql) {
        try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            assertThrows(SqlBindingException.class, () -> binder.bind(parser.parse(sql),
                    context(statement, Optional.of(ObjectName.of("app")))));
        }
    }

    private void bind(TransactionMetadataScope transaction, String sql) {
        try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            binder.bind(parser.parse(sql), context(statement, Optional.of(ObjectName.of("app"))));
        }
    }

    private static SqlBindingContext context(StatementBindingScope statement, Optional<ObjectName> schema) {
        return new SqlBindingContext(schema, ZoneId.of("Asia/Shanghai"), statement);
    }
}
