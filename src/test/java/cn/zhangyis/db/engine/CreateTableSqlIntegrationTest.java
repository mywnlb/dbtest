package cn.zhangyis.db.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
import cn.zhangyis.db.dd.domain.ColumnGeneration;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.session.SessionOptions;
import cn.zhangyis.db.sql.executor.CommandResult;
import cn.zhangyis.db.sql.executor.QueryResult;
import cn.zhangyis.db.sql.executor.UpdateResult;
import cn.zhangyis.db.sql.executor.storage.SqlDurabilityMode;
import cn.zhangyis.db.sql.executor.storage.SqlIsolationLevel;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStorageException;
import cn.zhangyis.db.sql.type.SqlValue;
import cn.zhangyis.db.storage.engine.EngineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CREATE TABLE SQL 从 Session 到原子 DD/physical DDL 的生产链验收；测试不通过 Java facade
 * 预建目标表，避免把底层能力误判为 SQL 已接线。
 */
class CreateTableSqlIntegrationTest {

    @TempDir
    Path directory;

    /**
     * SQL 建表必须完成 implicit commit 边界、schema 默认字符属性解析、物理索引创建和重启恢复，
     * 返回后目标表立即可由普通 DML 使用。
     */
    @Test
    void createsUsableTableThroughSessionAndSurvivesRestart() {
        EngineConfig config = config();
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(70_001), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(2));
            try (var session = database.openSession(options(false))) {
                CommandResult result = assertInstanceOf(CommandResult.class, session.execute("""
                        CREATE TABLE accounts (
                          id BIGINT PRIMARY KEY,
                          email VARCHAR(160) NOT NULL DEFAULT 'unknown@example.test',
                          note TEXT NULL,
                          UNIQUE INDEX uk_email (email)
                        )
                        """));
                assertTrue(result.transactionStatus().transactionActive(),
                        "autocommit=0 的 DDL 结束后必须恢复新的 implicit transaction");

                session.execute("""
                        INSERT INTO accounts (id, email, note)
                        VALUES (1, 'first@example.test', 'created by SQL')
                        """);
                session.execute("COMMIT");
                assertEquals(1, assertInstanceOf(QueryResult.class,
                        session.execute("SELECT note FROM accounts WHERE id=1")).rows().size());
            }
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(70_002), QualifiedTableName.of("app", "accounts"),
                    TableAccessIntent.READ, Duration.ofSeconds(2))) {
                assertEquals(2, lease.table().indexes().size());
                assertFalse(lease.table().columns().getFirst().type().nullable());
                assertEquals(1, lease.table().columns().get(1).type().charsetId());
                assertEquals(2, lease.table().columns().get(1).type().collationId());
                assertEquals(ColumnDefaultDefinition.constant("'unknown@example.test'"),
                        lease.table().columns().get(1).defaultDefinition());
            }
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var session = reopened.openSession(options(true))) {
                assertEquals(1, assertInstanceOf(QueryResult.class,
                        session.execute("SELECT email FROM accounts WHERE id=1")).rows().size());
            }
        }
    }

    /**
     * 二表 INNER JOIN 必须经过 Session→Parser→Binder→Optimizer→JoinNode→双 storage cursor
     * 的生产链；普通二级 inner probe 支持多匹配，WHERE、排序、LIMIT 和重复结果列名均在连接后生效。
     */
    @Test
    void executesIndexedInnerJoinThroughSession() {
        try (DatabaseEngine database =
                     new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(70_030),
                    ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(2));
            try (var session =
                         database.openSession(
                                 options(true))) {
                session.execute("""
                        CREATE TABLE customers (
                          id INT PRIMARY KEY,
                          name VARCHAR(64) NOT NULL
                        )
                        """);
                session.execute("""
                        CREATE TABLE orders (
                          id BIGINT PRIMARY KEY,
                          customer_id INT NOT NULL,
                          amount INT NOT NULL,
                          INDEX idx_customer (customer_id)
                        )
                        """);
                session.execute("""
                        INSERT INTO customers (id, name)
                        VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')
                        """);
                session.execute("""
                        INSERT INTO orders (id, customer_id, amount)
                        VALUES (10, 1, 15), (11, 1, 30),
                               (12, 2, 5), (13, 9, 99)
                        """);

                QueryResult result = assertInstanceOf(
                        QueryResult.class,
                        session.execute("""
                                SELECT c.name, o.amount
                                FROM customers AS c
                                INNER JOIN orders o
                                  ON c.id = o.customer_id
                                WHERE o.amount > 10
                                ORDER BY o.amount DESC LIMIT 2
                                """));
                assertEquals(List.of("name", "amount"),
                        result.columns().stream()
                                .map(column -> column.name())
                                .toList());
                assertEquals(List.of(
                                List.of(
                                        new SqlValue.StringValue("Alice"),
                                        new SqlValue.IntegerValue(
                                                java.math.BigInteger.valueOf(30))),
                                List.of(
                                        new SqlValue.StringValue("Alice"),
                                        new SqlValue.IntegerValue(
                                                java.math.BigInteger.valueOf(15)))),
                        result.rows().stream()
                                .map(row -> row.values())
                                .toList());

                QueryResult duplicateNames =
                        assertInstanceOf(
                                QueryResult.class,
                                session.execute("""
                                        SELECT c.id, o.id
                                        FROM customers c
                                        JOIN orders o
                                          ON c.id=o.customer_id
                                        WHERE o.amount > 20
                                        """));
                assertEquals(List.of("id", "id"),
                        duplicateNames.columns().stream()
                                .map(column -> column.name())
                                .toList(),
                        "SQL 允许多表投影产生重复显示列名");
                assertEquals(1,
                        duplicateNames.rows().size());
            }
        }
    }

    /**
     * 客户表完整导出语句必须通过 Binder、DD、Record schema 与物理 CREATE 全链路，而不仅是 Parser
     * 接受；整数显示宽度和 TINYINT(1) 均不得破坏存储类型不变量。
     */
    @Test
    void createsFullMysqlExportedCustomerTable() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(70_015), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(2));
            try (var session = database.openSession(options(true))) {
                assertInstanceOf(CommandResult.class, session.execute("""
                        CREATE TABLE `tb_customer` (
                          `id` bigint(20) NOT NULL COMMENT '主键ID',
                          `company_id` bigint(20) NOT NULL COMMENT '企业ID',
                          `customer_name` varchar(255) NOT NULL COMMENT '客户名称',
                          `customer_type` varchar(20) NOT NULL COMMENT '客户类型',
                          `customer_cate` varchar(20) DEFAULT NULL COMMENT '客户分类',
                          `customer_level` varchar(20) DEFAULT NULL COMMENT '客户级别',
                          `contact_name` varchar(50) DEFAULT NULL COMMENT '联系人',
                          `contact_phone` varchar(64) DEFAULT NULL COMMENT '联系人手机号',
                          `company_nature` varchar(20) DEFAULT NULL COMMENT '公司性质',
                          `company_industry` varchar(20) DEFAULT NULL COMMENT '所属行业',
                          `credit_level` varchar(20) DEFAULT NULL COMMENT '信用等级',
                          `customer_star` decimal(2,1) DEFAULT NULL COMMENT '客户星级',
                          `email` varchar(255) DEFAULT NULL COMMENT '邮箱',
                          `sex` varchar(20) DEFAULT NULL COMMENT '性别',
                          `certificate_type` varchar(20) DEFAULT NULL COMMENT '证件类型',
                          `certificate_cdoe` varchar(255) DEFAULT NULL COMMENT '证件号码',
                          `address` varchar(255) DEFAULT NULL COMMENT '地址',
                          `attachments` json DEFAULT NULL COMMENT '附件',
                          `status` tinyint(1) NOT NULL DEFAULT '1' COMMENT '状态(0:禁用,1:正常)',
                          `remark` varchar(200) DEFAULT NULL COMMENT '备注',
                          `update_time` datetime DEFAULT NULL COMMENT '更新时间',
                          `update_user` bigint(20) DEFAULT NULL COMMENT '更新人',
                          `create_time` datetime DEFAULT NULL COMMENT '创建时间',
                          `create_user` bigint(20) DEFAULT NULL COMMENT '创建人',
                          `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除：0-否,1-是',
                          PRIMARY KEY (`id`) USING BTREE
                        ) COMMENT='客户信息'
                        """));
            }
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(70_016),
                    QualifiedTableName.of("app", "tb_customer"),
                    TableAccessIntent.READ, Duration.ofSeconds(2))) {
                assertEquals(25, lease.table().columns().size());
                assertEquals("客户信息", lease.table().options().comment());
                assertEquals(0, lease.table().columns().getFirst().type().length());
                assertEquals(ColumnDefaultDefinition.constant("1"),
                        lease.table().columns().get(18).defaultDefinition());
            }
        }
    }

    /**
     * CREATE TABLE 的纯绑定错误必须早于 DDL implicit commit；否则重复列等客户端错误会意外提交
     * 当前用户事务，破坏 MySQL statement/transaction 边界。
     */
    @Test
    void rejectsInvalidCreateShapeBeforeImplicitCommit() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(70_010), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(2));
            try (var writer = database.openSession(options(false))) {
                writer.execute("CREATE TABLE seed (id BIGINT PRIMARY KEY)");
                writer.execute("INSERT INTO seed (id) VALUES (1)");

                assertThrows(DatabaseValidationException.class, () -> writer.execute(
                        "CREATE TABLE bad (id BIGINT PRIMARY KEY, ID INT)"));
                assertTrue(writer.snapshot().transactionActive(),
                        "纯绑定错误不得终结当前隐式事务");
                writer.execute("ROLLBACK");
            }
            try (var reader = database.openSession(options(true))) {
                assertTrue(assertInstanceOf(QueryResult.class,
                        reader.execute("SELECT id FROM seed WHERE id=1")).rows().isEmpty(),
                        "绑定错误后的显式回滚必须撤销此前未提交行");
            }
        }
    }

    /**
     * 设备关联表的完整 MySQL 导出语句必须从 SQL 接线创建真实表，并在 catalog/SDI 重启恢复后
     * 保留列注释、自增生成语义、数值默认值和表注释。
     */
    @Test
    void createsMysqlExportedAutoIncrementTableAndRestoresExtendedMetadata() {
        EngineConfig config = config();
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(70_020), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(2));
            try (var session = database.openSession(options(true))) {
                assertInstanceOf(CommandResult.class, session.execute("""
                        CREATE TABLE `tb_gw_device_relation` (
                          `id` bigint(15) NOT NULL AUTO_INCREMENT COMMENT 'id',
                          `product_key` varchar(50) DEFAULT '' COMMENT '设备productKey（接口返回）',
                          `device_key` varchar(50) DEFAULT '' COMMENT '设备deviceKey（接口返回）',
                          `business_type` varchar(50) DEFAULT '' COMMENT '业务类型（查看代码）',
                          `data_id` bigint(15) DEFAULT '-1' COMMENT '设备数据id（系统创建）',
                          PRIMARY KEY (`id`) USING BTREE,
                          UNIQUE KEY `product_key` (`product_key`,`device_key`) USING BTREE
                        ) COMMENT='格物设备数据关联表'
                        """));

                UpdateResult inserted = assertInstanceOf(UpdateResult.class,
                        session.execute("""
                                INSERT INTO tb_gw_device_relation
                                  (product_key, device_key, business_type)
                                VALUES ('p1', 'd1', 'first'),
                                       ('p2', 'd2', 'second')
                                """));
                assertEquals(2, inserted.affectedRows());
                assertEquals(java.math.BigInteger.ONE,
                        inserted.firstGeneratedKey().orElseThrow());
                assertEquals(new SqlValue.IntegerValue(
                                java.math.BigInteger.valueOf(-1)),
                        assertInstanceOf(QueryResult.class, session.execute(
                                "SELECT data_id FROM tb_gw_device_relation WHERE id=2"))
                                .rows().getFirst().values().getFirst());

                assertThrows(SqlStorageException.class,
                        () -> session.execute("""
                                INSERT INTO tb_gw_device_relation
                                  (product_key, device_key, business_type)
                                VALUES ('p3', 'd3', 'rolled-back'),
                                       ('p1', 'd1', 'duplicate')
                                """));
                assertTrue(assertInstanceOf(QueryResult.class, session.execute(
                        "SELECT id FROM tb_gw_device_relation WHERE product_key='p3'"))
                        .rows().isEmpty(),
                        "批次第二行失败必须回滚第一行");

                UpdateResult afterGap = assertInstanceOf(UpdateResult.class,
                        session.execute("""
                                INSERT INTO tb_gw_device_relation
                                  (product_key, device_key, business_type)
                                VALUES ('p4', 'd4', 'after-gap')
                                """));
                assertEquals(java.math.BigInteger.valueOf(5),
                        afterGap.firstGeneratedKey().orElseThrow(),
                        "失败批次消耗的 3/4 不得回收");

                UpdateResult mixed = assertInstanceOf(UpdateResult.class,
                        session.execute("""
                                INSERT INTO tb_gw_device_relation
                                  (id, product_key, device_key, business_type)
                                VALUES (NULL, 'p6', 'd6', 'generated'),
                                       (10, 'p10', 'd10', 'explicit'),
                                       (NULL, 'p11', 'd11', 'generated')
                                """));
                assertEquals(java.math.BigInteger.valueOf(6),
                        mixed.firstGeneratedKey().orElseThrow());
                assertEquals(3, mixed.affectedRows());
                assertEquals(1, assertInstanceOf(QueryResult.class,
                        session.execute(
                                "SELECT id FROM tb_gw_device_relation WHERE id=11"))
                        .rows().size());

                QueryResult indexOrdered = assertInstanceOf(
                        QueryResult.class, session.execute("""
                                SELECT id FROM tb_gw_device_relation WHERE id>=1
                                ORDER BY id ASC LIMIT 2 OFFSET 1
                                """));
                assertEquals(List.of(
                                new SqlValue.IntegerValue(java.math.BigInteger.valueOf(2)),
                                new SqlValue.IntegerValue(java.math.BigInteger.valueOf(5))),
                        indexOrdered.rows().stream()
                                .map(row -> row.values().getFirst()).toList(),
                        "聚簇索引顺序满足 ORDER BY 时应直接流式 LIMIT");

                QueryResult topN = assertInstanceOf(
                        QueryResult.class, session.execute("""
                                SELECT id FROM tb_gw_device_relation WHERE id>=1
                                ORDER BY id DESC LIMIT 2 OFFSET 1
                                """));
                assertEquals(List.of(
                                new SqlValue.IntegerValue(java.math.BigInteger.valueOf(10)),
                                new SqlValue.IntegerValue(java.math.BigInteger.valueOf(6))),
                        topN.rows().stream()
                                .map(row -> row.values().getFirst()).toList(),
                        "反向顺序当前不能由正向索引证明，必须由 Top-N 稳定排序");

                QueryResult partitioned = assertInstanceOf(
                        QueryResult.class, session.execute("""
                                SELECT id, business_type
                                FROM tb_gw_device_relation WHERE id>=1
                                ORDER BY business_type ASC, id ASC
                                """));
                assertEquals(List.of(5L, 10L, 1L, 6L, 11L, 2L),
                        partitioned.rows().stream()
                                .map(row -> ((SqlValue.IntegerValue)
                                        row.values().getFirst()).value().longValueExact())
                                .toList(),
                        "非索引、无限制 ORDER BY 必须走分堆 run 归并");
            }
            assertExtendedTableMetadata(database);
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            assertExtendedTableMetadata(reopened);
            try (var session = reopened.openSession(options(true))) {
                UpdateResult restarted = assertInstanceOf(UpdateResult.class,
                        session.execute("""
                                INSERT INTO tb_gw_device_relation
                                  (product_key, device_key, business_type)
                                VALUES ('p5', 'd5', 'restart')
                                """));
                assertEquals(java.math.BigInteger.valueOf(12),
                        restarted.firstGeneratedKey().orElseThrow());
            }
        }
    }

    /**
     * 自增 high-water 的物理字段虽可表达无符号 64 位，但发号边界必须服从实际列类型。该测试先把
     * signed TINYINT 推进到 127，再验证生成请求在记录 DML 前失败且原有行保持可见。
     */
    @Test
    void stopsAutoIncrementAtDeclaredIntegerTypeMaximum() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(70_025), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(2));
            try (var session = database.openSession(options(true))) {
                session.execute("""
                        CREATE TABLE signed_counter (
                          id TINYINT NOT NULL AUTO_INCREMENT,
                          PRIMARY KEY (id)
                        )
                        """);
                session.execute(
                        "INSERT INTO signed_counter (id) VALUES (127)");

                assertThrows(DatabaseValidationException.class,
                        () -> session.execute(
                                "INSERT INTO signed_counter (id) VALUES (NULL)"));
                assertEquals(1, assertInstanceOf(
                        QueryResult.class, session.execute(
                                "SELECT id FROM signed_counter WHERE id=127"))
                        .rows().size(),
                        "列域耗尽不得破坏已提交行或进入记录写入阶段");
            }
        }
    }

    /**
     * 基础 DDL v2 必须经 Session 执行 implicit commit、传播 warning，并把多表 DROP 的 pending/final
     * 集合作为一个字典批次发布；DROP 当前 schema 后会话选择立即清空，重启仍保持 tombstone。
     */
    @Test
    void executesSchemaWarningsAtomicMultiTableDropAndCascadeRestart() {
        EngineConfig config = config();
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            try (var session = database.openSession(
                    optionsFor("dropme", false))) {
                CommandResult created = assertInstanceOf(
                        CommandResult.class,
                        session.execute(
                                "CREATE DATABASE IF NOT EXISTS dropme"));
                assertTrue(created.warnings().isEmpty());
                CommandResult duplicate = assertInstanceOf(
                        CommandResult.class,
                        session.execute(
                                "CREATE SCHEMA IF NOT EXISTS dropme"));
                assertEquals(1007,
                        duplicate.warnings().getFirst().code());

                session.execute(
                        "CREATE TABLE first_t (id BIGINT PRIMARY KEY)");
                session.execute(
                        "CREATE TABLE second_t (id BIGINT PRIMARY KEY)");
                assertThrows(
                        cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException.class, () ->
                        session.execute(
                                "DROP TABLE first_t, absent_t"),
                        "无 IF 的缺失目标必须让整个批次保持无副作用");
                session.execute(
                        "INSERT INTO first_t (id) VALUES (1)");

                CommandResult dropped = assertInstanceOf(
                        CommandResult.class, session.execute(
                                "DROP TABLE IF EXISTS first_t, absent_t, second_t"));
                assertEquals(List.of(1051), dropped.warnings().stream()
                        .map(warning -> warning.code()).toList());
                assertThrows(
                        cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException.class, () ->
                        session.execute(
                                "SELECT id FROM first_t WHERE id=1"));

                session.execute(
                        "CREATE TABLE cascade_a (id BIGINT PRIMARY KEY)");
                session.execute(
                        "CREATE TABLE cascade_b (id BIGINT PRIMARY KEY)");
                CommandResult cascade = assertInstanceOf(
                        CommandResult.class,
                        session.execute("DROP DATABASE dropme"));
                assertTrue(cascade.warnings().isEmpty());
                assertTrue(session.snapshot().currentSchema().isEmpty());
                CommandResult missing = assertInstanceOf(
                        CommandResult.class,
                        session.execute(
                                "DROP SCHEMA IF EXISTS dropme"));
                assertEquals(1008,
                        missing.warnings().getFirst().code());
            }
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var session = reopened.openSession(options(true))) {
                assertEquals(0, assertInstanceOf(
                        CommandResult.class,
                        session.execute(
                                "CREATE SCHEMA dropme"))
                        .warnings().size(),
                        "DROPPED schema tombstone 不得阻止同名新 identity");
            }
        }
    }

    /**
     * @param database 已打开且包含 app.tb_gw_device_relation 的引擎
     */
    private static void assertExtendedTableMetadata(DatabaseEngine database) {
        try (var lease = database.dictionary().openTable(
                MdlOwnerId.of(70_021),
                QualifiedTableName.of("app", "tb_gw_device_relation"),
                TableAccessIntent.READ, Duration.ofSeconds(2))) {
            assertEquals("格物设备数据关联表", lease.table().options().comment());
            assertEquals(ColumnGeneration.AUTO_INCREMENT,
                    lease.table().columns().getFirst().generation());
            assertEquals("id", lease.table().columns().getFirst().comment());
            assertEquals(ColumnDefaultDefinition.constant("-1"),
                    lease.table().columns().getLast().defaultDefinition());
        }
    }

    /** @return 使用当前临时目录、固定页大小和有界资源预算的独立引擎配置。 */
    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 128,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    /**
     * @param autocommit 是否让普通 DML 使用 statement transaction；DDL 始终走独立 owner
     * @return current schema 为 app、所有等待均有界的 Session 配置
     */
    private static SessionOptions options(boolean autocommit) {
        return optionsFor("app", autocommit);
    }

    /**
     * @param schema Session 初始 current schema；允许在连接后由 SQL 创建
     * @param autocommit 是否使用 statement transaction
     * @return 使用固定隔离级别和有界 timeout 的 Session 配置
     */
    private static SessionOptions optionsFor(
            String schema, boolean autocommit) {
        return new SessionOptions(Optional.of(schema), autocommit,
                SqlIsolationLevel.REPEATABLE_READ, SqlDurabilityMode.FLUSH_ON_COMMIT,
                ZoneId.of("UTC"), Duration.ofSeconds(3), Duration.ofSeconds(2),
                Duration.ofSeconds(2), Duration.ofSeconds(2));
    }
}
