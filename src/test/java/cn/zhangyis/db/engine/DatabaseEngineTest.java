package cn.zhangyis.db.engine;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
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
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.dd.sdi.DictionarySdiCodec;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.sdi.SdiPageLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import cn.zhangyis.db.session.SessionOptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DatabaseEngine 组合根 TDD：DD discovery 必须自动给 StorageEngine 提供重启恢复表空间。 */
class DatabaseEngineTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path directory;

    /** 公共组合根必须显式拒绝空 prepared decision provider，默认构造器则安装 fail-closed provider。 */
    @Test
    void preparedDecisionProviderIsValidatedWithoutBreakingDefaultConstructor() {
        assertThrows(DatabaseValidationException.class,
                () -> new DatabaseEngine(config(), null));
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            assertEquals(DatabaseEngineState.OPEN, database.state());
        }
    }

    /** Session registry 只在完整 recovery+DDL recovery 后发布；其它生命周期状态一律拒绝。 */
    @Test
    void opensSessionsOnlyWhileEngineIsOpen() {
        DatabaseEngine database = new DatabaseEngine(config());
        assertThrows(DatabaseRuntimeException.class, () -> database.openSession(SessionOptions.defaults()));
        database.open();
        var session = database.openSession(SessionOptions.defaults());
        assertEquals(DatabaseEngineState.OPEN, database.state());
        session.close();
        database.close();
        assertEquals(DatabaseEngineState.CLOSED, database.state());
        assertThrows(DatabaseRuntimeException.class, () -> database.openSession(SessionOptions.defaults()));
    }

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
        corruptControlByte(directory.resolve("mysql.dd.ctrl"), 2L * 4096 - 1);

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var lease = reopened.dictionary().openTable(MdlOwnerId.of(2), name,
                    TableAccessIntent.READ, Duration.ofSeconds(2))) {
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
                    .openTable(MdlOwnerId.of(4), name, TableAccessIntent.READ, Duration.ofSeconds(1)));
        }
    }

    /**
     * v1 尚未给 CREATE SCHEMA 写 DDL marker；control 在该操作后回退时，字典版本下界仍必须阻止后续 TABLE marker
     * 复用已经分配给 schema 的 ddl id。
     */
    @Test
    void reconcilesDdlHighWaterAcrossUnloggedSchemaOperation() {
        EngineConfig config = config();
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(5), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            database.ddl().createTable(MdlOwnerId.of(5), tableCommand("app", "orders"),
                    Duration.ofSeconds(5));
            database.ddl().createSchema(MdlOwnerId.of(5), ObjectName.of("audit"), 1, 1,
                    Duration.ofSeconds(5));
        }

        // generation 4 位于 slot 0；破坏它后回退到 table CREATE 后的 generation 3，nextDdlId 会退回 3。
        corruptControlByte(directory.resolve("mysql.dd.ctrl"), 4096 - 1L);

        try (DictionaryControlStore fallback = DictionaryControlStore.openExisting(
                directory.resolve("mysql.dd.ctrl"), SpaceId.of(1))) {
            assertEquals(3, fallback.snapshot().nextDdlId(),
                    "the surviving generation must expose the reused-id risk");
        }
        try (DatabaseEngine reconciler = new DatabaseEngine(config)) {
            reconciler.open();
        }
        try (DictionaryControlStore reconciled = DictionaryControlStore.openExisting(
                directory.resolve("mysql.dd.ctrl"), SpaceId.of(1))) {
            assertTrue(reconciled.snapshot().nextDdlId() > 3,
                    "committed dictionary evidence must conservatively advance unlogged DDL identities");
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            var table = reopened.ddl().createTable(MdlOwnerId.of(6), tableCommand("audit", "events"),
                    Duration.ofSeconds(5));
            assertEquals("events", table.name().canonicalName());
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

    /**
     * page-level checksum 仍有效但 SDI payload CRC 已损坏时，启动必须以 committed DD 重写 page3；
     * 修复后读取到的完整聚合必须与 catalog 当前版本一致。
     */
    @Test
    void rewritesLogicallyCorruptedSdiFromCommittedDictionaryOnRestart() {
        EngineConfig config = config();
        Path tableFile;
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(20), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            tableFile = database.ddl().createTable(MdlOwnerId.of(20),
                    tableCommand("app", "orders"), Duration.ofSeconds(5))
                    .storageBinding().orElseThrow().path();
        }
        rewritePage(tableFile, SdiPageLayout.PAGE_NO,
                page -> page.put(SdiPageLayout.PAYLOAD_OFFSET,
                        (byte) (page.get(SdiPageLayout.PAYLOAD_OFFSET) ^ 0x5A)));

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var lease = reopened.dictionary().openTable(MdlOwnerId.of(21),
                    QualifiedTableName.of("app", "orders"), TableAccessIntent.READ,
                    Duration.ofSeconds(2))) {
                var binding = lease.table().storageBinding().orElseThrow();
                var repaired = reopened.storage().tableDdlStorageService()
                        .readSerializedDictionaryInfo(binding).orElseThrow();
                assertEquals(lease.table().version().value(), repaired.dictionaryVersion());
                assertEquals(lease.table(), new DictionarySdiCodec().decode(repaired.payload()));
            }
        }
    }

    /**
     * 升级前 GENERAL 表空间的 page0 root=0 是唯一 legacy 空态；只要 committed binding 完整，
     * recovery 必须原地建立 page3/root，不要求用户重建表。
     */
    @Test
    void upgradesLegacyTablespaceWithoutSdiRootOnRestart() {
        EngineConfig config = config();
        Path tableFile;
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(22), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            tableFile = database.ddl().createTable(MdlOwnerId.of(22),
                    tableCommand("app", "orders"), Duration.ofSeconds(5))
                    .storageBinding().orElseThrow().path();
        }
        rewritePage(tableFile, 0, page -> page.putLong(SpaceHeaderLayout.SDI_ROOT, 0L));

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var lease = reopened.dictionary().openTable(MdlOwnerId.of(23),
                    QualifiedTableName.of("app", "orders"), TableAccessIntent.READ,
                    Duration.ofSeconds(2))) {
                var stored = reopened.storage().tableDdlStorageService()
                        .readSerializedDictionaryInfo(lease.table().storageBinding().orElseThrow())
                        .orElseThrow();
                assertEquals(lease.table(), new DictionarySdiCodec().decode(stored.payload()));
            }
        }
        assertEquals(SdiPageLayout.PAGE_NO,
                readPage(tableFile, 0).getLong(SpaceHeaderLayout.SDI_ROOT));
    }

    /**
     * v1 只认识 root 0/3；其它指针可能指向业务页，不能因 committed DD 存在就覆盖，
     * 启动必须保持 FAILED 并保留文件供诊断。
     */
    @Test
    void failsClosedForUnknownSdiRootInsteadOfOverwritingAnotherPage() {
        EngineConfig config = config();
        Path tableFile;
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(24), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            tableFile = database.ddl().createTable(MdlOwnerId.of(24),
                    tableCommand("app", "orders"), Duration.ofSeconds(5))
                    .storageBinding().orElseThrow().path();
        }
        rewritePage(tableFile, 0, page -> page.putLong(SpaceHeaderLayout.SDI_ROOT, 99L));

        DatabaseEngine reopened = new DatabaseEngine(config);
        assertThrows(DictionaryRecoveryException.class, reopened::open);
        assertEquals(DatabaseEngineState.FAILED, reopened.state());
        assertTrue(Files.exists(tableFile));
        reopened.close();
    }

    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    private static CreateTableCommand tableCommand(String schema, String table) {
        return new CreateTableCommand(QualifiedTableName.of(schema, table), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"),
                        ColumnTypeDefinition.bigint(false, false))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                        List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0)))));
    }

    private static void corruptControlByte(Path path, long position) {
        try (FileChannel control = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(1);
            if (control.read(byteBuffer, position) != 1) {
                throw new AssertionError("control corruption target is unreadable: " + position);
            }
            byteBuffer.flip();
            byte damaged = (byte) (byteBuffer.get() ^ 0xFF);
            control.write(ByteBuffer.wrap(new byte[]{damaged}), position);
            control.force(true);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * 修改一张已关闭表空间页并重新盖统一页 checksum，使测试只破坏目标逻辑字段，
     * 不会提前被 Buffer Pool 的物理 torn-page 校验拦截。
     */
    private static void rewritePage(Path path, long pageNo, java.util.function.Consumer<ByteBuffer> mutation) {
        byte[] bytes = readPage(path, pageNo).array();
        ByteBuffer view = ByteBuffer.wrap(bytes);
        mutation.accept(view);
        PageImageChecksum.stamp(bytes, PAGE_SIZE);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer source = ByteBuffer.wrap(bytes);
            long position = pageNo * PAGE_SIZE.bytes();
            while (source.hasRemaining()) {
                position += channel.write(source, position);
            }
            channel.force(true);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    /** 完整读取指定物理页并保留 absolute-offset 访问语义。 */
    private static ByteBuffer readPage(Path path, long pageNo) {
        ByteBuffer page = ByteBuffer.allocate(PAGE_SIZE.bytes());
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long position = pageNo * PAGE_SIZE.bytes();
            while (page.hasRemaining()) {
                int read = channel.read(page, position);
                if (read < 0) {
                    throw new AssertionError("unexpected EOF reading tablespace page " + pageNo);
                }
                position += read;
            }
            return page;
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }
}
