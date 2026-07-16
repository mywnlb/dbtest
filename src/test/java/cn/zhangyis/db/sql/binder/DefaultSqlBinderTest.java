package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPrimaryPointSelect;
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
                    parser.parse("INSERT INTO ORDERS (NOTE, ID, TENANT) VALUES ('x', 7, 2)"),
                    context(statement, Optional.of(ObjectName.of("app")))));
            assertEquals(List.of(new SqlValue.IntegerValue(BigInteger.valueOf(7)),
                    new SqlValue.IntegerValue(BigInteger.valueOf(2)), new SqlValue.StringValue("x")),
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
            BoundPrimaryPointSelect bound = assertInstanceOf(BoundPrimaryPointSelect.class, binder.bind(
                    parser.parse("SELECT Note, ID FROM def.APP.ORDERS WHERE TENANT=2 AND ID=7"),
                    context(statement, Optional.empty())));
            assertEquals(List.of(2, 0), bound.projectionOrdinals());
            assertEquals(List.of(new SqlValue.IntegerValue(BigInteger.valueOf(7)),
                    new SqlValue.IntegerValue(BigInteger.valueOf(2))), bound.keyValues());
        }
    }

    /** v1 不允许不完整/多余主键、prefix/Lob 主键、缺列 INSERT 或无物理 binding。 */
    @Test
    void rejectsUnsupportedShapesWithoutPublishingLeases() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                     MdlOwnerId.of(202))) {
            assertBindFails(transaction, "SELECT * FROM orders WHERE id=1");
            assertBindFails(transaction, "SELECT * FROM orders WHERE id=1 AND tenant=2 AND note='x'");
            assertBindFails(transaction, "INSERT INTO orders (id, tenant) VALUES (1, 2)");
            assertBindFails(transaction, "SELECT * FROM prefix_key WHERE code='x'");
            assertBindFails(transaction, "SELECT * FROM lob_key WHERE body='x'");
            assertBindFails(transaction, "SELECT * FROM unbound WHERE id=1");
            assertTrue(fixture.locks.snapshot().granted().isEmpty());
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

    private void assertBindFails(TransactionMetadataScope transaction, String sql) {
        try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
            assertThrows(SqlBindingException.class, () -> binder.bind(parser.parse(sql),
                    context(statement, Optional.of(ObjectName.of("app")))));
        }
    }

    private static SqlBindingContext context(StatementBindingScope statement, Optional<ObjectName> schema) {
        return new SqlBindingContext(schema, ZoneId.of("Asia/Shanghai"), statement);
    }
}
