package cn.zhangyis.db.engine;

import cn.zhangyis.db.dd.ddl.CreateColumnSpec;
import cn.zhangyis.db.dd.ddl.CreateIndexKeyPartSpec;
import cn.zhangyis.db.dd.ddl.CreateIndexSpec;
import cn.zhangyis.db.dd.ddl.CreateTableCommand;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DatabaseEngine 组合根 TDD：DD discovery 必须自动给 StorageEngine 提供重启恢复表空间。 */
class DatabaseEngineTest {

    @TempDir
    Path directory;

    /** 建表关闭后无需调用方手工配置 recoveryTablespaces 即可重开 root；DROP 后再次重开仍保持不可见。 */
    @Test
    void discoversDictionaryTablespacesAcrossRestartAndRecoversDropState() {
        EngineConfig config = config();
        QualifiedTableName name = QualifiedTableName.of("app", "orders");
        Path tableFile;
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(1), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            var table = database.ddl().createTable(MdlOwnerId.of(1), new CreateTableCommand(name,
                    PageNo.of(128),
                    List.of(new CreateColumnSpec(ObjectName.of("id"),
                            ColumnTypeDefinition.bigint(false, false))),
                    List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                            List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))))),
                    Duration.ofSeconds(5));
            tableFile = table.storageBinding().orElseThrow().path();
            assertTrue(Files.exists(tableFile));
        }

        // 模拟 control latest slot torn/corrupt：重开会回退上一代，但必须由 committed catalog 抬高所有 next-counter。
        try (FileChannel control = FileChannel.open(directory.resolve("mysql.dd.ctrl"), StandardOpenOption.WRITE)) {
            control.write(ByteBuffer.wrap(new byte[]{0}), 2L * 4096 - 1);
            control.force(true);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var lease = reopened.dictionary().openTable(MdlOwnerId.of(2), name, Duration.ofSeconds(2))) {
                var binding = lease.table().storageBinding().orElseThrow().indexes().getFirst();
                MiniTransaction read = reopened.storage().miniTransactionManager().beginReadOnly();
                assertEquals(binding.indexId(), reopened.storage().indexPageAccess()
                        .openIndexPage(read, binding.rootPageId(), PageLatchMode.SHARED).header().indexId());
                reopened.storage().miniTransactionManager().commit(read);
            }
            var second = reopened.ddl().createTable(MdlOwnerId.of(3), new CreateTableCommand(
                    QualifiedTableName.of("app", "audit"), PageNo.of(128),
                    List.of(new CreateColumnSpec(ObjectName.of("id"),
                            ColumnTypeDefinition.bigint(false, false))),
                    List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                            List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))))),
                    Duration.ofSeconds(5));
            assertTrue(second.storageBinding().orElseThrow().spaceId().value() > 1024,
                    "catalog reconciliation must prevent reuse of space 1024 after control fallback");
            reopened.ddl().dropTable(MdlOwnerId.of(3), name, Duration.ofSeconds(5));
            reopened.ddl().dropTable(MdlOwnerId.of(3), QualifiedTableName.of("app", "audit"),
                    Duration.ofSeconds(5));
            assertFalse(Files.exists(tableFile));
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            assertThrows(DictionaryObjectNotFoundException.class, () -> reopened.dictionary()
                    .openTable(MdlOwnerId.of(4), name, Duration.ofSeconds(1)));
        }
    }

    /** committed ACTIVE binding 缺少物理文件不能被当成 orphan/drop 成功，启动必须保持 FAILED。 */
    @Test
    void failsClosedWhenCommittedActiveTablespaceIsMissing() {
        EngineConfig config = config();
        Path tableFile;
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(11), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            tableFile = database.ddl().createTable(MdlOwnerId.of(11), new CreateTableCommand(
                    QualifiedTableName.of("app", "orders"), PageNo.of(128),
                    List.of(new CreateColumnSpec(ObjectName.of("id"),
                            ColumnTypeDefinition.bigint(false, false))),
                    List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                            List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))))),
                    Duration.ofSeconds(5)).storageBinding().orElseThrow().path();
        }
        try {
            Files.delete(tableFile);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }

        DatabaseEngine reopened = new DatabaseEngine(config);
        assertThrows(DictionaryRecoveryException.class, reopened::open);
        assertEquals(DatabaseEngineState.FAILED, reopened.state());
        reopened.close();
    }

    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }
}
