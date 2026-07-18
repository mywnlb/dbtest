package cn.zhangyis.db.session;

import cn.zhangyis.db.dd.ddl.*;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.sql.executor.QueryResult;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.executor.UpdateResult;
import cn.zhangyis.db.sql.executor.storage.SqlIsolationLevel;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStorageException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 只经 DatabaseEngine.openSession/SqlSession.execute 验收 SQL→DD→storage→公开值 round-trip。 */
class SqlSessionEndToEndTest {
    @TempDir Path directory;

    /** 28 DD 类型、复合主键、非 UTC TIMESTAMP 与多 external LOB 的完整往返。 */
    @Test
    void roundTripsAllDictionaryTypesAndExternalLobs() {
        try (DatabaseEngine database = new DatabaseEngine(SqlSessionTestSupport.config(directory))) {
            database.open();
            SqlSessionTestSupport.createSchema(database);
            createAllTypesTable(database);
            try (SqlSession session = database.openSession(SqlSessionTestSupport.options(true,
                    SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(2)))) {
                Values values = values();
                UpdateResult inserted = assertInstanceOf(UpdateResult.class,
                        session.execute(insertSql(values.literals)));
                assertEquals(1, inserted.affectedRows());
                QueryResult selected = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT * FROM all_types WHERE c_bigint=1 AND c_int=7"));
                assertEquals(1, selected.rows().size());
                assertEquals(values.expected, selected.rows().getFirst().values());
            }
        }
    }

    /** 同一事务跨两个索引写入后 full rollback 必须按 undo identity resolver 全部撤销；BEGIN 会先提交 implicit。 */
    @Test
    void handlesAutocommitImplicitBeginAndCrossTableRollback() {
        try (DatabaseEngine database = new DatabaseEngine(SqlSessionTestSupport.config(directory))) {
            database.open();
            SqlSessionTestSupport.createSchema(database);
            SqlSessionTestSupport.createSimpleTable(database, "left_t", 10_001);
            SqlSessionTestSupport.createSimpleTable(database, "right_t", 10_002);
            try (SqlSession session = database.openSession(SqlSessionTestSupport.options(false,
                    SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(2)))) {
                session.execute("INSERT INTO left_t (id) VALUES (1)");
                session.execute("INSERT INTO right_t (id) VALUES (1)");
                session.execute("ROLLBACK");
                assertTrue(query(session, "left_t", 1).rows().isEmpty());
                assertTrue(query(session, "right_t", 1).rows().isEmpty());

                session.execute("INSERT INTO left_t (id) VALUES (2)");
                session.execute("BEGIN"); // 隐式提交 id=2，再开启 explicit。
                session.execute("INSERT INTO right_t (id) VALUES (2)");
                session.execute("ROLLBACK");
                assertEquals(1, query(session, "left_t", 2).rows().size());
                assertTrue(query(session, "right_t", 2).rows().isEmpty());
            }
        }
    }

    /**
     * 真实 Session 链路返回 non-unique prefix 多行并 hydrate LOB；显式 FOR UPDATE 持 logical/clustered 锁，
     * 同 prefix INSERT 超时，COMMIT 后同一 Session 可重试成功。
     */
    @Test
    void executesNonUniqueRangeAndHoldsLockingReadUntilCommit() {
        try (DatabaseEngine database = new DatabaseEngine(SqlSessionTestSupport.config(directory))) {
            database.open();
            SqlSessionTestSupport.createSchema(database);
            createRangeTable(database);
            String firstBody = "一".repeat(300);
            String secondBody = "二".repeat(320);
            try (SqlSession seed = database.openSession(SqlSessionTestSupport.options(true,
                    SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(2)))) {
                seed.execute("INSERT INTO range_docs (id,category,body) VALUES "
                        + "(1,'team','" + firstBody + "')");
                seed.execute("INSERT INTO range_docs (id,category,body) VALUES "
                        + "(2,'TEAM','" + secondBody + "')");
                QueryResult range = assertInstanceOf(QueryResult.class,
                        seed.execute("SELECT body,id FROM range_docs WHERE category='team'"));
                assertEquals(2, range.rows().size());
                assertEquals(new SqlValue.StringValue(firstBody), range.rows().get(0).values().get(0));
                assertEquals(new SqlValue.StringValue(secondBody), range.rows().get(1).values().get(0));
            }

            try (SqlSession locker = database.openSession(SqlSessionTestSupport.options(true,
                    SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(2)));
                 SqlSession writer = database.openSession(SqlSessionTestSupport.options(true,
                         SqlIsolationLevel.REPEATABLE_READ, Duration.ofMillis(100)))) {
                locker.execute("BEGIN");
                QueryResult locked = assertInstanceOf(QueryResult.class,
                        locker.execute("SELECT id FROM range_docs WHERE category='TEAM' FOR UPDATE"));
                assertEquals(2, locked.rows().size());

                assertThrows(SqlStorageException.class, () -> writer.execute(
                        "INSERT INTO range_docs (id,category,body) VALUES (3,'team','blocked')"));
                locker.execute("COMMIT");
                assertEquals(1, assertInstanceOf(UpdateResult.class, writer.execute(
                        "INSERT INTO range_docs (id,category,body) VALUES (3,'team','released')")).affectedRows());
                assertEquals(3, assertInstanceOf(QueryResult.class,
                        writer.execute("SELECT id FROM range_docs WHERE category='team'")).rows().size());
            }
        }
    }

    private static QueryResult query(SqlSession session, String table, long id) {
        return assertInstanceOf(QueryResult.class,
                session.execute("SELECT * FROM " + table + " WHERE id=" + id));
    }

    private static void createAllTypesTable(DatabaseEngine database) {
        List<CreateColumnSpec> columns = new ArrayList<>();
        columns.add(column("c_tinyint", scalar(DictionaryTypeId.TINYINT)));
        columns.add(column("c_smallint", scalar(DictionaryTypeId.SMALLINT)));
        columns.add(column("c_int", scalar(DictionaryTypeId.INT)));
        columns.add(column("c_bigint", scalar(DictionaryTypeId.BIGINT)));
        columns.add(column("c_float", scalar(DictionaryTypeId.FLOAT)));
        columns.add(column("c_double", scalar(DictionaryTypeId.DOUBLE)));
        columns.add(column("c_decimal", type(DictionaryTypeId.DECIMAL, 10, 2, List.of())));
        columns.add(column("c_char", type(DictionaryTypeId.CHAR, 4, 0, List.of())));
        columns.add(column("c_varchar", type(DictionaryTypeId.VARCHAR, 32, 0, List.of())));
        columns.add(column("c_binary", type(DictionaryTypeId.BINARY, 2, 0, List.of())));
        columns.add(column("c_varbinary", type(DictionaryTypeId.VARBINARY, 16, 0, List.of())));
        columns.add(column("c_date", scalar(DictionaryTypeId.DATE)));
        columns.add(column("c_datetime", scalar(DictionaryTypeId.DATETIME)));
        columns.add(column("c_time", scalar(DictionaryTypeId.TIME)));
        columns.add(column("c_timestamp", scalar(DictionaryTypeId.TIMESTAMP)));
        columns.add(column("c_year", scalar(DictionaryTypeId.YEAR)));
        columns.add(column("c_bit", type(DictionaryTypeId.BIT, 5, 0, List.of())));
        columns.add(column("c_enum", type(DictionaryTypeId.ENUM, 3, 0, List.of("a", "b", "c"))));
        columns.add(column("c_set", type(DictionaryTypeId.SET, 3, 0, List.of("a", "b", "c"))));
        columns.add(column("c_tinytext", type(DictionaryTypeId.TINYTEXT, 255, 0, List.of())));
        columns.add(column("c_text", type(DictionaryTypeId.TEXT, 65_535, 0, List.of())));
        columns.add(column("c_mediumtext", type(DictionaryTypeId.MEDIUMTEXT, 16_777_215, 0, List.of())));
        columns.add(column("c_longtext", type(DictionaryTypeId.LONGTEXT, Integer.MAX_VALUE, 0, List.of())));
        columns.add(column("c_tinyblob", type(DictionaryTypeId.TINYBLOB, 255, 0, List.of())));
        columns.add(column("c_blob", type(DictionaryTypeId.BLOB, 65_535, 0, List.of())));
        columns.add(column("c_mediumblob", type(DictionaryTypeId.MEDIUMBLOB, 16_777_215, 0, List.of())));
        columns.add(column("c_longblob", type(DictionaryTypeId.LONGBLOB, Integer.MAX_VALUE, 0, List.of())));
        columns.add(column("c_json", type(DictionaryTypeId.JSON, Integer.MAX_VALUE, 0, List.of())));
        database.ddl().createTable(MdlOwnerId.of(10_010), new CreateTableCommand(
                QualifiedTableName.of("app", "all_types"), PageNo.of(256), columns,
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true, List.of(
                        new CreateIndexKeyPartSpec(ObjectName.of("c_bigint"), IndexOrder.ASC, 0),
                        new CreateIndexKeyPartSpec(ObjectName.of("c_int"), IndexOrder.ASC, 0))))),
                Duration.ofSeconds(5));
    }

    /** 创建单列 ordinary secondary + external TEXT，供 range MVCC、locking 与 LOB 同链验收。 */
    private static void createRangeTable(DatabaseEngine database) {
        database.ddl().createTable(MdlOwnerId.of(10_020), new CreateTableCommand(
                QualifiedTableName.of("app", "range_docs"), PageNo.of(384),
                List.of(
                        new CreateColumnSpec(ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false)),
                        new CreateColumnSpec(ObjectName.of("category"), new ColumnTypeDefinition(
                                DictionaryTypeId.VARCHAR, false, false, 32, 0, 1, 2, List.of())),
                        new CreateColumnSpec(ObjectName.of("body"), type(DictionaryTypeId.TEXT,
                                65_535, 0, List.of()))),
                List.of(
                        new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                                List.of(new CreateIndexKeyPartSpec(
                                        ObjectName.of("id"), IndexOrder.ASC, 0))),
                        new CreateIndexSpec(ObjectName.of("idx_category"), false, false,
                                List.of(new CreateIndexKeyPartSpec(
                                        ObjectName.of("category"), IndexOrder.ASC, 0))))),
                Duration.ofSeconds(5));
    }

    private static CreateColumnSpec column(String name, ColumnTypeDefinition type) {
        return new CreateColumnSpec(ObjectName.of(name), type);
    }

    private static ColumnTypeDefinition scalar(DictionaryTypeId type) {
        return new ColumnTypeDefinition(type, false, false, 0, 0, 0, 0, List.of());
    }

    private static ColumnTypeDefinition type(DictionaryTypeId type, int length, int scale, List<String> symbols) {
        int charset = switch (type) {
            case CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, JSON, ENUM, SET -> 1;
            default -> 0;
        };
        int collation = charset == 0 ? 0 : 1;
        return new ColumnTypeDefinition(type, false, false, length, scale, charset, collation, symbols);
    }

    private static Values values() {
        String externalText = "长".repeat(300);
        byte[] externalBlob = new byte[300];
        for (int i = 0; i < externalBlob.length; i++) externalBlob[i] = (byte) i;
        String externalJson = "{\"payload\":\"" + "x".repeat(300) + "\"}";
        List<String> literals = List.of("-128", "32767", "7", "1", "1.5", "-2.25", "12.34",
                "'c'", "'var'", "X'ABCD'", "X'0102'", "'2024-01-02'",
                "'2024-01-02 03:04:05.006'", "'-12:34:56.007'", "'2024-01-02 03:04:05.006'",
                "2155", "B'10101'", "'b'", "'a,c'", "'tiny'", "'" + externalText + "'",
                "'medium'", "'long'", "X'CAFE'", "X'" + hex(externalBlob) + "'", "X'BEEF'",
                "X'DEAD'", "'" + externalJson + "'");
        Instant timestamp = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 6_000_000)
                .atZone(ZoneId.of("Asia/Shanghai")).toInstant();
        List<SqlValue> expected = List.of(
                integer(-128), integer(32767), integer(7), integer(1), new SqlValue.FloatingValue(1.5),
                new SqlValue.FloatingValue(-2.25), new SqlValue.DecimalValue(new BigDecimal("12.34")),
                new SqlValue.StringValue("c"), new SqlValue.StringValue("var"),
                new SqlValue.BytesValue(new byte[]{(byte) 0xAB, (byte) 0xCD}),
                new SqlValue.BytesValue(new byte[]{1, 2}),
                new SqlValue.TemporalValue(SqlValue.TemporalKind.DATE, LocalDate.of(2024, 1, 2).toEpochDay()),
                new SqlValue.TemporalValue(SqlValue.TemporalKind.DATETIME,
                        LocalDateTime.of(2024, 1, 2, 3, 4, 5, 6_000_000).toInstant(ZoneOffset.UTC).toEpochMilli()),
                new SqlValue.TemporalValue(SqlValue.TemporalKind.TIME, -45_296_007L),
                new SqlValue.TemporalValue(SqlValue.TemporalKind.TIMESTAMP, timestamp.toEpochMilli()),
                new SqlValue.TemporalValue(SqlValue.TemporalKind.YEAR, 2155),
                new SqlValue.BitValue(new byte[]{(byte) 0xA8}, 5), new SqlValue.EnumValue("b", 2),
                new SqlValue.SetValue(List.of("a", "c"), 5), new SqlValue.StringValue("tiny"),
                new SqlValue.StringValue(externalText), new SqlValue.StringValue("medium"),
                new SqlValue.StringValue("long"), new SqlValue.BytesValue(new byte[]{(byte) 0xCA, (byte) 0xFE}),
                new SqlValue.BytesValue(externalBlob), new SqlValue.BytesValue(new byte[]{(byte) 0xBE, (byte) 0xEF}),
                new SqlValue.BytesValue(new byte[]{(byte) 0xDE, (byte) 0xAD}),
                new SqlValue.StringValue(externalJson));
        return new Values(literals, expected);
    }

    private static SqlValue.IntegerValue integer(long value) {
        return new SqlValue.IntegerValue(BigInteger.valueOf(value));
    }

    private static String insertSql(List<String> literals) {
        List<String> columns = List.of("c_tinyint", "c_smallint", "c_int", "c_bigint", "c_float", "c_double",
                "c_decimal", "c_char", "c_varchar", "c_binary", "c_varbinary", "c_date", "c_datetime",
                "c_time", "c_timestamp", "c_year", "c_bit", "c_enum", "c_set", "c_tinytext", "c_text",
                "c_mediumtext", "c_longtext", "c_tinyblob", "c_blob", "c_mediumblob", "c_longblob", "c_json");
        return "INSERT INTO all_types (" + String.join(",", columns) + ") VALUES ("
                + String.join(",", literals) + ")";
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) result.append(String.format(java.util.Locale.ROOT, "%02X", b & 0xFF));
        return result.toString();
    }

    private record Values(List<String> literals, List<SqlValue> expected) { }
}
