package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.sql.binder.bound.BoundInsert;
import cn.zhangyis.db.sql.binder.bound.BoundJoinSelect;
import cn.zhangyis.db.sql.binder.bound.BoundSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
import cn.zhangyis.db.sql.binder.bound.BoundCreateTable;
import cn.zhangyis.db.sql.binder.bound.BoundDropIndex;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
import cn.zhangyis.db.dd.domain.ColumnGeneration;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundNegation;
import cn.zhangyis.db.sql.expression.BoundNullTest;
import cn.zhangyis.db.sql.expression.BoundNullTestOperator;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.parser.ast.CreateIndexStatementNode;
import cn.zhangyis.db.sql.parser.ast.CreateTableStatementNode;
import cn.zhangyis.db.sql.parser.ast.DropIndexStatementNode;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;
import cn.zhangyis.db.sql.binder.exception.UnsupportedSqlShapeException;
import cn.zhangyis.db.sql.type.SqlValue;
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

/** 验证 Binder 只负责名称、类型和 SQL 语义，不再选择任何物理访问路径。 */
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
            BoundInsert bound = assertInstanceOf(BoundInsert.class, binder.bind(
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

    /**
     * 多行 INSERT 必须共享同一列映射，并为每行独立补齐隐式 NULL；任一行宽错误不能发布
     * metadata scope 或部分 batch。
     */
    @Test
    void bindsMultiRowInsertAndFillsMissingNullableColumns() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(
                     fixture.dictionary, MdlOwnerId.of(220));
             StatementBindingScope statement =
                     transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundInsert bound = assertInstanceOf(BoundInsert.class, binder.bind(
                    parser.parse("""
                            INSERT INTO orders (id, tenant, status)
                            VALUES (1, 10, 'open'), (2, 20, 'closed')
                            """),
                    context(statement, Optional.of(ObjectName.of("app")))));

            assertEquals(2, bound.batch().rows().size());
            assertEquals(4, bound.batch().width());
            assertEquals(SqlValue.NullValue.INSTANCE,
                    ((cn.zhangyis.db.sql.type.InsertValueSource.Constant)
                            bound.batch().rows().getLast().get(2)).value());
        }
    }

    /** 三段名与投影被规范化，谓词保持 SQL conjunction 顺序而不按索引 key part 重排。 */
    @Test
    void bindsThreePartPrimaryPointSelect() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(201));
             StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundSelect bound = assertInstanceOf(BoundSelect.class, binder.bind(
                    parser.parse("SELECT Note, ID FROM def.APP.ORDERS WHERE TENANT=2 AND ID=7"),
                    context(statement, Optional.empty())));
            List<BoundComparison> predicates = comparisons(bound.condition());
            assertEquals(List.of(2, 0), bound.projectionOrdinals());
            assertEquals(List.of(new SqlValue.IntegerValue(BigInteger.valueOf(7)),
                            new SqlValue.IntegerValue(BigInteger.valueOf(2))),
                    List.of(literalValue(predicates.get(1)),
                            literalValue(predicates.get(0))));
            assertEquals(List.of(1, 0), predicates.stream()
                    .map(DefaultSqlBinderTest::columnOrdinal).toList());
        }
    }

    /**
     * ORDER BY 可以引用未投影列，但必须绑定到 exact DD column id；LIMIT 原样冻结，
     * 物理索引与排序策略仍不属于 Binder。
     */
    @Test
    void bindsOrderByUnprojectedColumnAndLimit() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(
                     fixture.dictionary, MdlOwnerId.of(221));
             StatementBindingScope statement =
                     transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundSelect bound = assertInstanceOf(BoundSelect.class, binder.bind(
                    parser.parse("""
                            SELECT id FROM orders WHERE tenant=2
                            ORDER BY status DESC, id LIMIT 4 OFFSET 3
                            """),
                    context(statement, Optional.of(ObjectName.of("app")))));

            assertEquals(List.of(3, 0), bound.orderBy().stream()
                    .map(key -> key.columnOrdinal()).toList());
            assertEquals(List.of(IndexOrder.DESC, IndexOrder.ASC),
                    bound.orderBy().stream().map(key -> key.direction()).toList());
            assertEquals(bound.table().columns().get(3).columnId(),
                    bound.orderBy().getFirst().columnId());
            assertEquals(3L, bound.limit().orElseThrow().offset());
            assertEquals(4L, bound.limit().orElseThrow().count());
        }
    }

    /**
     * JOIN Binder 必须把两个 relation ordinal 和扁平投影固定到 exact DD version，
     * ON 保持列列比较，WHERE 保持独立 residual。
     */
    @Test
    void bindsQualifiedInnerJoinAndFlattenedProjection() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(
                     fixture.dictionary, MdlOwnerId.of(222));
             StatementBindingScope statement =
                     transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundJoinSelect bound = assertInstanceOf(
                    BoundJoinSelect.class, binder.bind(
                            parser.parse("""
                                    SELECT o.id, c.note
                                    FROM orders o
                                    JOIN customers c ON o.tenant = c.id
                                    WHERE c.status = 'open'
                                    ORDER BY o.id LIMIT 5
                                    """),
                            context(statement,
                                    Optional.of(ObjectName.of("app")))));

            assertEquals(List.of("orders", "customers"),
                    bound.tables().stream()
                            .map(table -> table.name().canonicalName())
                            .toList());
            assertEquals(List.of(0, 5), bound.projectionOrdinals());
            BoundComparison on = assertInstanceOf(
                    BoundComparison.class, bound.joinCondition());
            assertEquals(List.of(0, 1),
                    List.of(((cn.zhangyis.db.sql.expression.BoundColumnReference)
                                    on.left()).relationOrdinal(),
                            ((cn.zhangyis.db.sql.expression.BoundColumnReference)
                                    on.right()).relationOrdinal()));
            assertEquals(0, bound.orderBy().getFirst().columnOrdinal());
            assertEquals(5L, bound.limit().orElseThrow().count());
        }
    }

    /** 未限定同名列必须报歧义；未知 qualifier 和重复 alias 也不能发布 metadata scope。 */
    @Test
    void rejectsAmbiguousAndInvalidJoinNames() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(
                     fixture.dictionary, MdlOwnerId.of(223))) {
            assertJoinBindingFails(fixture, transaction,
                    "SELECT id FROM orders o JOIN customers c ON o.tenant=c.id WHERE c.status='open'",
                    "ambiguous");
            assertJoinBindingFails(fixture, transaction,
                    "SELECT x.id FROM orders o JOIN customers c ON o.tenant=c.id WHERE c.status='open'",
                    "qualifier");
            assertJoinBindingFails(fixture, transaction,
                    "SELECT o.id FROM orders o JOIN customers o ON o.tenant=o.id WHERE o.status='open'",
                    "alias");
        }
    }

    private void assertJoinBindingFails(
            BinderTestFixture fixture,
            TransactionMetadataScope transaction,
            String sql, String messagePart) {
        try (StatementBindingScope statement =
                     transaction.beginStatement(Duration.ofSeconds(1))) {
            SqlBindingException failure = assertThrows(
                    SqlBindingException.class,
                    () -> binder.bind(parser.parse(sql),
                            context(statement,
                                    Optional.of(ObjectName.of("app")))));
            assertTrue(failure.getMessage().toLowerCase()
                    .contains(messagePart));
        }
    }

    /**
     * DDL v2 Binder 只做名称规范化和列表去重，不读取对象存在性；重复目标必须在 implicit commit 前失败。
     */
    @Test
    void bindsSchemaAndAtomicDropTableCommands() {
        var create = binder.bindDdl(assertInstanceOf(
                cn.zhangyis.db.sql.parser.ast.CreateSchemaStatementNode.class,
                parser.parse("CREATE DATABASE IF NOT EXISTS Sales")));
        assertEquals("sales", create.name().canonicalName());
        assertTrue(create.ifNotExists());

        var drop = binder.bindDdl(assertInstanceOf(
                        cn.zhangyis.db.sql.parser.ast.DropTableStatementNode.class,
                        parser.parse("DROP TABLE IF EXISTS t1, app.t2")),
                Optional.of(ObjectName.of("app")));
        assertEquals(List.of("def.app.t1", "def.app.t2"),
                drop.tables().stream()
                        .map(name -> name.canonicalKey()).toList());

        var duplicate = assertInstanceOf(
                cn.zhangyis.db.sql.parser.ast.DropTableStatementNode.class,
                parser.parse("DROP TABLE t1, APP.T1"));
        assertThrows(DatabaseValidationException.class,
                () -> binder.bindDdl(
                        duplicate, Optional.of(ObjectName.of("app"))));
    }

    /** unique secondary 形状在 Binder 中与其它 SELECT 相同，只留下 typed predicate。 */
    @Test
    void bindsCompleteUniqueSecondaryPointSelect() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(204));
             StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundSelect bound = assertInstanceOf(BoundSelect.class, binder.bind(
                    parser.parse("SELECT ID, NOTE FROM orders WHERE NOTE='Hello'"),
                    context(statement, Optional.of(ObjectName.of("app")))));
            BoundComparison predicate = comparisons(bound.condition()).getFirst();
            assertEquals(List.of(0, 2), bound.projectionOrdinals());
            assertEquals(2, columnOrdinal(predicate));
            assertEquals(new SqlValue.StringValue("Hello"),
                    literalValue(predicate));
        }
    }

    /** non-unique equality 仍是普通语义 SELECT；locking clause 同时提升 metadata intent。 */
    @Test
    void bindsNonUniqueSecondaryRangeAndLockMode() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(207))) {
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundSelect bound = assertInstanceOf(BoundSelect.class, binder.bind(
                        parser.parse("SELECT id, status FROM orders WHERE status='open'"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(List.of(0, 3), bound.projectionOrdinals());
                assertEquals(new SqlValue.StringValue("open"),
                        literalValue(comparisons(
                                bound.condition()).getFirst()));
                assertEquals(SelectLockMode.CONSISTENT, bound.lockMode());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundSelect bound = assertInstanceOf(BoundSelect.class, binder.bind(
                        parser.parse("SELECT * FROM orders WHERE status='open' FOR UPDATE"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(SelectLockMode.FOR_UPDATE, bound.lockMode());
                assertEquals(4, bound.projectionOrdinals().size());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundSelect bound = assertInstanceOf(BoundSelect.class, binder.bind(
                        parser.parse("SELECT id FROM orders WHERE status='open' FOR SHARE"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(SelectLockMode.FOR_SHARE, bound.lockMode());
            }
        }
    }

    /**
     * Binder 展开 comparison 并保留 residual，但不选择复合索引，也不把 DESC 转为物理边界。
     */
    @Test
    void bindsCompositeComparisonRangeAndResidualPredicates() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(208));
             StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            BoundSelect bound = assertInstanceOf(BoundSelect.class, binder.bind(
                    parser.parse("""
                            SELECT id, tenant FROM orders
                            WHERE status='open' AND tenant>=2 AND tenant<9
                            """),
                    context(statement, Optional.of(ObjectName.of("app")))));

            List<BoundComparison> predicates = comparisons(bound.condition());
            assertEquals(3, predicates.size());
            assertEquals(BoundComparisonOperator.GREATER_THAN_OR_EQUAL,
                    predicates.get(1).operator());
            assertEquals(BoundComparisonOperator.LESS_THAN,
                    predicates.get(2).operator());
        }
    }

    /**
     * Binder 必须递归保留 OR/NOT/null-test 的 typed 语义树；相同列出现在不同 OR
     * 分支是合法 SQL，不能沿用 M2 的全局重复 equality 拒绝策略。
     */
    @Test
    void bindsBooleanExpressionTreeWithoutChoosingAccessPath() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(
                     fixture.dictionary, MdlOwnerId.of(212))) {
            try (StatementBindingScope statement =
                         transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundSelect bound = assertInstanceOf(
                        BoundSelect.class, binder.bind(
                        parser.parse("""
                                SELECT id FROM orders
                                WHERE id=7 AND (note IS NULL OR NOT status='closed')
                                """),
                        context(statement,
                                Optional.of(ObjectName.of("app")))));

                BoundConjunction conjunction = assertInstanceOf(
                        BoundConjunction.class, bound.condition());
                BoundDisjunction disjunction = assertInstanceOf(
                        BoundDisjunction.class,
                        conjunction.operands().getLast());
                BoundNullTest nullTest = assertInstanceOf(
                        BoundNullTest.class,
                        disjunction.operands().getFirst());
                assertEquals(BoundNullTestOperator.IS_NULL,
                        nullTest.operator());
                assertTrue(nullTest.operand().type().nullable());
                assertInstanceOf(BoundNegation.class,
                        disjunction.operands().getLast());
            }
            assertDoesNotThrow(() -> bind(transaction,
                    "SELECT id FROM orders WHERE id=7 OR id=8"));
        }
    }

    /** 无可用索引和矛盾范围在 Binder 中仍保持同一语义形状，empty 证明属于 Optimizer。 */
    @Test
    void bindsClusteredFullScanAndDetectsEmptyIntersection() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(209))) {
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundSelect fullScan = assertInstanceOf(BoundSelect.class, binder.bind(
                        parser.parse("SELECT id FROM orders WHERE tenant>2"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(1, comparisons(fullScan.condition()).size());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundSelect contradictory = assertInstanceOf(BoundSelect.class, binder.bind(
                        parser.parse("SELECT id FROM orders WHERE id>10 AND id<=10"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(2, comparisons(
                        contradictory.condition()).size());
            }
        }
    }

    /** UPDATE/DELETE 统一冻结语义谓词，赋值仍禁止触碰聚簇 key。 */
    @Test
    void bindsRangeUpdateAndDeleteWithoutLosingPointCompatibility() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(210))) {
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundUpdate update = assertInstanceOf(BoundUpdate.class, binder.bind(
                        parser.parse("UPDATE orders SET note='archived' WHERE status='old' AND tenant>=2"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(List.of(2), update.assignmentOrdinals());
                assertEquals(2, comparisons(update.condition()).size());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundDelete delete = assertInstanceOf(BoundDelete.class, binder.bind(
                        parser.parse("DELETE FROM orders WHERE tenant BETWEEN 2 AND 8"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                List<BoundComparison> predicates =
                        comparisons(delete.condition());
                assertEquals(2, predicates.size());
                assertEquals(BoundComparisonOperator.GREATER_THAN_OR_EQUAL,
                        predicates.getFirst().operator());
            }
        }
    }

    /** 主键形状的 UPDATE/DELETE 仍只产生语义谓词；Optimizer 才按 index part 形成 point key。 */
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
                assertEquals(List.of(1, 0),
                        comparisons(update.condition()).stream()
                                .map(DefaultSqlBinderTest::columnOrdinal)
                                .toList());
            }
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                BoundDelete delete = assertInstanceOf(BoundDelete.class, binder.bind(
                        parser.parse("DELETE FROM orders WHERE tenant=2 AND id=7"),
                        context(statement, Optional.of(ObjectName.of("app")))));
                assertEquals(List.of(1, 0),
                        comparisons(delete.condition()).stream()
                                .map(DefaultSqlBinderTest::columnOrdinal)
                                .toList());
            }
        }
    }

    /**
     * Binder 拒绝主键/重复赋值和重复 equality，但接受可由 Optimizer 安全降为 range 的非完整主键形状。
     */
    @Test
    void rejectsUnsafeAssignmentsButAcceptsSemanticRangeShapesWithoutPublishingLeases() {
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
            assertDoesNotThrow(() -> bind(transaction, "DELETE FROM prefix_key WHERE code='x'"));
            assertBindFails(transaction, "DELETE FROM lob_key WHERE body='x'");
            assertTrue(fixture.locks.snapshot().granted().isEmpty(),
                    "未由 Session publish 的 Binder scope 关闭后不得留下 MDL");
        }
    }

    /** Binder 接受可表达的 prefix-key 语义；LOB residual、缺列 INSERT 和无物理 binding 仍前置拒绝。 */
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
            assertDoesNotThrow(() -> bind(transaction, "SELECT * FROM prefix_key WHERE code='x'"));
            assertBindFails(transaction, "SELECT * FROM lob_key WHERE body='x'");
            assertBindFails(transaction, "SELECT * FROM unbound WHERE id=1");
            assertTrue(fixture.locks.snapshot().granted().isEmpty(),
                    "未由 Session publish 的 Binder scope 关闭后不得留下 MDL");
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

    /**
     * CREATE TABLE Binder 只把纯语法转换为稳定 DD command：主键列强制 NOT NULL、字符列保留
     * schema-default 继承哨兵，常量 default 在任何 implicit commit 前完成类型验证。
     */
    @Test
    void bindsCreateTableToAtomicDictionaryCommandWithoutMetadataLease() {
        BoundCreateTable bound = binder.bindDdl(
                assertInstanceOf(CreateTableStatementNode.class, parser.parse("""
                        CREATE TABLE accounts (
                          id BIGINT PRIMARY KEY,
                          email VARCHAR(160) NOT NULL DEFAULT 'unknown@example.test',
                          UNIQUE INDEX uk_email (email DESC)
                        )
                        """)),
                Optional.of(ObjectName.of("app")), ZoneId.of("Asia/Shanghai"));

        assertEquals("def.app.accounts", bound.command().name().canonicalKey());
        assertEquals(128, bound.command().initialSizeInPages().value());
        assertFalse(bound.command().columns().getFirst().type().nullable());
        assertEquals(DictionaryTypeId.VARCHAR,
                bound.command().columns().get(1).type().typeId());
        assertEquals(0, bound.command().columns().get(1).type().charsetId(),
                "Binder 不能在未持 schema MDL 时猜测 charset");
        assertEquals(ColumnDefaultDefinition.constant("'unknown@example.test'"),
                bound.command().columns().get(1).defaultDefinition());
        assertEquals(List.of(IndexOrder.ASC, IndexOrder.DESC),
                bound.command().indexes().stream()
                        .map(index -> index.keyParts().getFirst().order()).toList());

        assertThrows(DatabaseValidationException.class, () -> binder.bindDdl(
                assertInstanceOf(CreateTableStatementNode.class,
                        parser.parse("CREATE TABLE bad (id BIGINT PRIMARY KEY, ID INT)")),
                Optional.of(ObjectName.of("app")), ZoneId.of("UTC")));
        assertThrows(DatabaseValidationException.class, () -> binder.bindDdl(
                assertInstanceOf(CreateTableStatementNode.class,
                        parser.parse("CREATE TABLE bad (id BIGINT)")),
                Optional.of(ObjectName.of("app")), ZoneId.of("UTC")));
        assertThrows(UnsupportedSqlShapeException.class, () -> binder.bindDdl(
                assertInstanceOf(CreateTableStatementNode.class,
                        parser.parse("CREATE TABLE bad (id BIGINT DEFAULT NULL PRIMARY KEY)")),
                Optional.of(ObjectName.of("app")), ZoneId.of("UTC")));
    }

    /**
     * MySQL 导出的整数显示宽度必须被归一化，数值列的引号默认值必须 canonical 化，
     * 注释、自增与表选项则必须完整进入 DD command。
     */
    @Test
    void bindsMysqlExportedCreateTableMetadataWithoutLosingSemanticFields() {
        BoundCreateTable bound = binder.bindDdl(
                assertInstanceOf(CreateTableStatementNode.class, parser.parse("""
                        CREATE TABLE `tb_gw_device_relation` (
                          `id` bigint(15) NOT NULL AUTO_INCREMENT COMMENT 'id',
                          `product_key` varchar(50) DEFAULT '' COMMENT '设备productKey（接口返回）',
                          `device_key` varchar(50) DEFAULT '' COMMENT '设备deviceKey（接口返回）',
                          `business_type` varchar(50) DEFAULT '' COMMENT '业务类型（查看代码）',
                          `data_id` bigint(15) DEFAULT '-1' COMMENT '设备数据id（系统创建）',
                          PRIMARY KEY (`id`) USING BTREE,
                          UNIQUE KEY `product_key` (`product_key`,`device_key`) USING BTREE
                        ) COMMENT='格物设备数据关联表'
                        """)),
                Optional.of(ObjectName.of("app")), ZoneId.of("Asia/Shanghai"));

        assertEquals(0, bound.command().columns().getFirst().type().length(),
                "BIGINT 显示宽度不能泄漏为物理长度");
        assertEquals(ColumnGeneration.AUTO_INCREMENT,
                bound.command().columns().getFirst().generation());
        assertEquals("id", bound.command().columns().getFirst().comment());
        assertEquals(ColumnDefaultDefinition.constant("-1"),
                bound.command().columns().getLast().defaultDefinition());
        assertEquals("格物设备数据关联表", bound.command().options().comment());
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

    /**
     * 把 Binder 输出的完整 condition 展开为测试需要观察的 comparison 顺序。
     *
     * @param condition Binder 产生的唯一权威 boolean 表达式
     * @return 按用户书写顺序展开且不可变的 comparison 列表
     */
    private static List<BoundComparison> comparisons(
            BoundExpression condition) {
        return PredicateSet.of(condition).conjuncts().stream()
                .map(BoundComparison.class::cast)
                .toList();
    }

    /**
     * 读取规范 column-literal comparison 的列位置。
     *
     * @param comparison Binder 已规范化的 comparison
     * @return exact table version 中的零基列位置
     */
    private static int columnOrdinal(BoundComparison comparison) {
        return assertInstanceOf(
                cn.zhangyis.db.sql.expression.BoundColumnReference.class,
                comparison.left()).columnOrdinal();
    }

    /**
     * 读取规范 column-literal comparison 的 typed literal。
     *
     * @param comparison Binder 已规范化的 comparison
     * @return coercion 后的 SQL 值，SQL NULL 保持显式 NullValue
     */
    private static SqlValue literalValue(BoundComparison comparison) {
        return assertInstanceOf(
                cn.zhangyis.db.sql.expression.BoundLiteral.class,
                comparison.right()).value();
    }
}
