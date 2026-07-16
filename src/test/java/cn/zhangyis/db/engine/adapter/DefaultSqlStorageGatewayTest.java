package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.dd.ddl.*;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPrimaryPointSelect;
import cn.zhangyis.db.sql.executor.SqlRow;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.executor.storage.*;
import cn.zhangyis.db.sql.executor.storage.exception.SqlTransactionStateException;
import cn.zhangyis.db.storage.engine.EngineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** SQL port→真实 transaction/DML/MVCC/LOB 的 adapter 集成 TDD。 */
class DefaultSqlStorageGatewayTest {
    @TempDir Path directory;

    /** exact bound INSERT 真实进入 storage，提交后点查按投影顺序返回并 hydrate external TEXT。 */
    @Test
    void insertsCommitsAndReadsHydratedLobThroughOpaqueHandle() {
        try (DatabaseEngine database = openDatabase()) {
            TableDefinition table = createTable(database);
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(2));
            String text = "长".repeat(300);
            BoundClusteredInsert insert = new BoundClusteredInsert(table, List.of(
                    new SqlValue.IntegerValue(BigInteger.ONE), new SqlValue.StringValue(text)));
            SqlTransactionHandle write = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            assertEquals(1, gateway.insert(write, insert).affectedRows());
            assertTrue(gateway.commit(write, new SqlCommitRequest(SqlDurabilityMode.FLUSH_ON_COMMIT,
                    Duration.ofSeconds(2))).durable());
            assertThrows(SqlTransactionStateException.class, () -> gateway.rollback(write));

            SqlTransactionHandle read = gateway.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.READ_COMMITTED, true, true));
            BoundPrimaryPointSelect select = new BoundPrimaryPointSelect(table, List.of(1, 0),
                    List.of(new SqlValue.IntegerValue(BigInteger.ONE)));
            SqlRow row = gateway.selectPoint(read, select).orElseThrow();
            assertEquals(List.of(new SqlValue.StringValue(text),
                    new SqlValue.IntegerValue(BigInteger.ONE)), row.values());
            gateway.commit(read, new SqlCommitRequest(SqlDurabilityMode.BACKGROUND_FLUSH,
                    Duration.ofSeconds(1)));
        }
    }

    /** 无 undo 事务可直接 rollback；跨 gateway、重复终结和终态 handle 必须 fail-closed。 */
    @Test
    void validatesHandleOwnershipAndTerminalState() {
        try (DatabaseEngine database = openDatabase()) {
            DefaultSqlStorageGateway first = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(1));
            DefaultSqlStorageGateway second = new DefaultSqlStorageGateway(database.storage(),
                    new DictionaryStorageMetadataMapper(), Duration.ofSeconds(1));
            SqlTransactionHandle handle = first.begin(new SqlTransactionRequest(
                    SqlIsolationLevel.REPEATABLE_READ, false, false));
            assertThrows(SqlTransactionStateException.class, () -> second.rollback(handle));
            assertEquals(0, first.rollback(handle).undoneRecords());
            assertThrows(SqlTransactionStateException.class, () -> first.rollback(handle));
            assertThrows(SqlTransactionStateException.class, () -> first.commit(handle,
                    new SqlCommitRequest(SqlDurabilityMode.BACKGROUND_FLUSH, Duration.ofSeconds(1))));
        }
    }

    private DatabaseEngine openDatabase() {
        DatabaseEngine database = new DatabaseEngine(new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(10), 64L * 1024 * 1024));
        database.open();
        return database;
    }

    private static TableDefinition createTable(DatabaseEngine database) {
        database.ddl().createSchema(MdlOwnerId.of(300), ObjectName.of("app"), 1, 1, Duration.ofSeconds(2));
        TableDefinition created = database.ddl().createTable(MdlOwnerId.of(300), new CreateTableCommand(
                QualifiedTableName.of("app", "docs"), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false)),
                        new CreateColumnSpec(ObjectName.of("body"), new ColumnTypeDefinition(DictionaryTypeId.TEXT,
                                false, false, 65_535, 0, 1, 1, List.of()))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                        List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))))),
                Duration.ofSeconds(5));
        try (var lease = database.dictionary().openTable(MdlOwnerId.of(301), QualifiedTableName.of("app", "docs"),
                TableAccessIntent.WRITE, Duration.ofSeconds(2))) {
            assertEquals(created.version(), lease.version());
            return lease.table();
        }
    }
}
