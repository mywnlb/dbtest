package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.dd.ddl.CreateColumnSpec;
import cn.zhangyis.db.dd.ddl.CreateIndexKeyPartSpec;
import cn.zhangyis.db.dd.ddl.CreateIndexSpec;
import cn.zhangyis.db.dd.ddl.CreateTableCommand;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.fil.catalog.FileDictionaryRecoveryManifestStore;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryManifestRepository;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * catalog-loss recovery 端到端测试：使用真实 DDL 表空间、full-page scrub、隔离和 baseline 重新启动。
 */
class CatalogRecoveryServiceTest {

    @TempDir
    Path directory;

    /**
     * 零长度 catalog 与伪装 extra candidate 必须先由 token 显式隔离；重建后普通 DatabaseEngine
     * 仍走严格 catalog open，并能按原 schema/table 名读取 DD。
     */
    @Test
    void quarantinesExplicitConflictsAndRebuildsCatalogForNormalOpen() throws IOException {
        Path tablePath = createHealthyTable();
        Path catalog = directory.resolve("mysql.ibd");
        try (FileChannel channel = FileChannel.open(catalog, StandardOpenOption.WRITE)) {
            channel.truncate(0);
            channel.force(true);
        }
        Path extra = directory.resolve("tables").resolve("table_999_space_1999.ibd");
        Files.copy(tablePath, extra, StandardCopyOption.COPY_ATTRIBUTES);

        CatalogRecoveryService recovery = new CatalogRecoveryService(config());
        CatalogRecoveryInspection blocked = recovery.inspect(Duration.ofSeconds(20));

        assertEquals(CatalogRecoveryStatus.BLOCKED, blocked.status());
        assertTrue(blocked.conflicts().stream().anyMatch(
                conflict -> conflict.kind() == CatalogRecoveryConflictKind.CATALOG_EMPTY));
        assertTrue(blocked.conflicts().stream().anyMatch(
                conflict -> conflict.kind() == CatalogRecoveryConflictKind.EXTRA_CANDIDATE_INVALID));
        Set<CatalogRecoveryConflictId> selected = blocked.conflicts().stream()
                .filter(CatalogRecoveryConflict::quarantinable)
                .map(CatalogRecoveryConflict::id)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());

        CatalogRecoveryInspection rebuildable = recovery.quarantine(
                blocked.token().orElseThrow(), selected, Duration.ofSeconds(20));
        assertEquals(CatalogRecoveryStatus.REBUILDABLE, rebuildable.status(),
                rebuildable.conflicts().toString());
        assertFalse(Files.exists(catalog));
        assertFalse(Files.exists(extra));

        CatalogRebuildResult rebuilt = recovery.rebuild(
                rebuildable.token().orElseThrow(), Duration.ofSeconds(20));
        assertEquals(1, rebuilt.schemaCount());
        assertEquals(1, rebuilt.tableCount());
        assertTrue(Files.isRegularFile(catalog));

        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(2), QualifiedTableName.of("app", "orders"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ,
                    Duration.ofSeconds(5))) {
                assertEquals(ObjectName.of("orders"), lease.table().name());
            }
        }
    }

    /** catalog 与 manifest 同时丢失时不得从单表 SDI 猜 schema，且不能签发 quarantine/rebuild token。 */
    @Test
    void blocksCatalogLossWithoutManifest() throws IOException {
        createHealthyTable();
        Files.delete(directory.resolve("mysql.ibd"));
        Files.delete(directory.resolve("mysql.dd.manifest"));

        CatalogRecoveryInspection inspection =
                new CatalogRecoveryService(config()).inspect(Duration.ofSeconds(20));

        assertEquals(CatalogRecoveryStatus.BLOCKED, inspection.status());
        assertTrue(inspection.token().isEmpty());
        assertTrue(inspection.conflicts().stream().anyMatch(
                conflict -> conflict.kind() == CatalogRecoveryConflictKind.MANIFEST_MISSING));
    }

    /**
     * 非空但双头不可恢复的 catalog/control 必须分别形成可隔离证据，不能被误分类为 missing，
     * 也不能被 rebuild 入口自动覆盖。
     */
    @Test
    void classifiesNonEmptyCorruptCatalogAndControlAsQuarantinableEvidence() throws IOException {
        createHealthyTable();
        overwritePrefixWithZeros(directory.resolve("mysql.ibd"), 8192);
        overwritePrefixWithZeros(directory.resolve("mysql.dd.ctrl"), 8192);

        CatalogRecoveryInspection inspection =
                new CatalogRecoveryService(config()).inspect(Duration.ofSeconds(20));

        assertEquals(CatalogRecoveryStatus.BLOCKED, inspection.status());
        CatalogRecoveryConflict catalog = inspection.conflicts().stream()
                .filter(conflict -> conflict.kind() == CatalogRecoveryConflictKind.CATALOG_CORRUPT)
                .findFirst().orElseThrow();
        CatalogRecoveryConflict control = inspection.conflicts().stream()
                .filter(conflict -> conflict.kind() == CatalogRecoveryConflictKind.CONTROL_CORRUPT)
                .findFirst().orElseThrow();
        assertTrue(catalog.quarantinable());
        assertTrue(control.quarantinable());
    }

    /**
     * complete directory scan 必须同时报告重复 tableId 与重复 spaceId；即使伪副本的页内 identity
     * 不匹配文件名，重复命名身份也不能被泛化的 EXTRA_CANDIDATE_INVALID 掩盖。
     */
    @Test
    void reportsDuplicateTableAndSpaceIdentitiesAlongsideInvalidExtras() throws IOException {
        Path expected = createHealthyTable();
        Path tables = directory.resolve("tables");
        Files.copy(expected, tables.resolve("table_1_space_1999.ibd"), StandardCopyOption.COPY_ATTRIBUTES);
        Files.copy(expected, tables.resolve("table_999_space_1024.ibd"), StandardCopyOption.COPY_ATTRIBUTES);
        Files.delete(directory.resolve("mysql.ibd"));

        CatalogRecoveryInspection inspection =
                new CatalogRecoveryService(config()).inspect(Duration.ofSeconds(20));

        assertEquals(CatalogRecoveryStatus.BLOCKED, inspection.status());
        assertTrue(inspection.conflicts().stream().anyMatch(
                conflict -> conflict.kind() == CatalogRecoveryConflictKind.DUPLICATE_TABLE_ID));
        assertTrue(inspection.conflicts().stream().anyMatch(
                conflict -> conflict.kind() == CatalogRecoveryConflictKind.DUPLICATE_SPACE_ID));
        assertEquals(2, inspection.conflicts().stream()
                .filter(conflict -> conflict.kind()
                        == CatalogRecoveryConflictKind.EXTRA_CANDIDATE_INVALID)
                .count());
    }

    /**
     * clean 后的 mutation intent 与 manifest 唯一期望文件缺失都必须阻塞；二者都不能由 quarantine
     * 绕过，避免工具把未裁决提交或数据丢失解释成可重建空缺。
     */
    @Test
    void blocksDirtyManifestAndMissingExpectedCandidate() throws IOException {
        Path table = createHealthyTable();
        Path manifestPath = directory.resolve("mysql.dd.manifest");
        try (DictionaryRecoveryManifestRepository manifest = new DictionaryRecoveryManifestRepository(
                FileDictionaryRecoveryManifestStore.openExisting(manifestPath))) {
            manifest.beforeCatalogMutation(DictionaryVersion.of(999), List.of(new CatalogRecord(
                    "injected-intent".getBytes(StandardCharsets.UTF_8),
                    "not-applied".getBytes(StandardCharsets.UTF_8))));
        }
        Files.delete(directory.resolve("mysql.ibd"));
        Files.delete(table);

        CatalogRecoveryInspection inspection =
                new CatalogRecoveryService(config()).inspect(Duration.ofSeconds(20));

        assertEquals(CatalogRecoveryStatus.BLOCKED, inspection.status());
        CatalogRecoveryConflict dirty = inspection.conflicts().stream()
                .filter(conflict -> conflict.kind() == CatalogRecoveryConflictKind.MANIFEST_DIRTY)
                .findFirst().orElseThrow();
        CatalogRecoveryConflict missing = inspection.conflicts().stream()
                .filter(conflict -> conflict.kind()
                        == CatalogRecoveryConflictKind.EXPECTED_CANDIDATE_MISSING)
                .findFirst().orElseThrow();
        assertFalse(dirty.quarantinable());
        assertFalse(missing.quarantinable());
    }

    /** 普通引擎持有实例锁期间，离线 recovery 必须有界失败，不能并发扫描或移动文件。 */
    @Test
    void refusesOfflineInspectionWhileDatabaseEngineIsOpen() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();

            assertThrows(CatalogRecoveryBusyException.class,
                    () -> new CatalogRecoveryService(config()).inspect(Duration.ofMillis(100)));
        }
    }

    /**
     * token 必须绑定 expected 文件的原始摘要；page3 被改写后旧 token 不能发布 catalog，
     * 新 inspection 则把 checksum 损坏分类为不可隔离的 expected candidate failure。
     */
    @Test
    void rejectsStaleTokenAndExpectedChecksumCorruption() throws IOException {
        Path table = createHealthyTable();
        Files.delete(directory.resolve("mysql.ibd"));
        CatalogRecoveryService recovery = new CatalogRecoveryService(config());
        CatalogRecoveryInspection before = recovery.inspect(Duration.ofSeconds(20));
        assertEquals(CatalogRecoveryStatus.REBUILDABLE, before.status());

        try (FileChannel channel = FileChannel.open(table, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[] {42}),
                    3L * config().pageSize().bytes() + 128);
            channel.force(true);
        }

        assertThrows(CatalogRecoveryStaleTokenException.class,
                () -> recovery.rebuild(before.token().orElseThrow(), Duration.ofSeconds(20)));
        CatalogRecoveryInspection corrupted = recovery.inspect(Duration.ofSeconds(20));
        assertEquals(CatalogRecoveryStatus.BLOCKED, corrupted.status());
        CatalogRecoveryConflict expectedFailure = corrupted.conflicts().stream()
                .filter(conflict -> conflict.kind()
                        == CatalogRecoveryConflictKind.EXPECTED_CANDIDATE_INVALID)
                .findFirst().orElseThrow();
        assertFalse(expectedFailure.quarantinable(),
                "manifest 唯一期望文件损坏不能通过隔离绕过");
    }

    /** control 与 catalog 同时缺失时，manifest reservation 高水位先重建 control，再发布 baseline。 */
    @Test
    void rebuildsMissingControlBeforeCatalog() throws IOException {
        createHealthyTable();
        Files.delete(directory.resolve("mysql.ibd"));
        Files.delete(directory.resolve("mysql.dd.ctrl"));
        CatalogRecoveryService recovery = new CatalogRecoveryService(config());

        CatalogRecoveryInspection inspection = recovery.inspect(Duration.ofSeconds(20));
        assertEquals(CatalogRecoveryStatus.REBUILDABLE, inspection.status());
        Path staleTemporary = temporaryCatalog(inspection.token().orElseThrow());
        Files.write(staleTemporary, new byte[] {1, 2, 3}, StandardOpenOption.CREATE_NEW);
        CatalogRebuildResult result = recovery.rebuild(
                inspection.token().orElseThrow(), Duration.ofSeconds(20));

        assertTrue(Files.isRegularFile(directory.resolve("mysql.dd.ctrl")));
        assertTrue(result.controlSnapshot().nextTableId() > 1);
        assertTrue(result.controlSnapshot().nextSpaceId() > 1024);
        try (var entries = Files.newDirectoryStream(
                directory.resolve("catalog-recovery").resolve("evidence"),
                staleTemporary.getFileName() + ".invalid.*")) {
            assertTrue(entries.iterator().hasNext(),
                    "损坏的受控 rebuild temp 应保留为 evidence 后再重试");
        }
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
        }
    }

    /**
     * catalog 有效时损坏 manifest 不是 catalog-loss：普通启动应先原子保留 sidecar 证据，再从权威
     * catalog/SDI 收敛并生成新 clean snapshot。
     */
    @Test
    void healthyCatalogPreservesAndRegeneratesCorruptManifest() throws IOException {
        createHealthyTable();
        Path manifest = directory.resolve("mysql.dd.manifest");
        try (FileChannel channel = FileChannel.open(manifest, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[16]), 0);
            channel.write(ByteBuffer.wrap(new byte[16]), 4096);
            channel.force(true);
        }

        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
        }

        Path evidence = directory.resolve("catalog-recovery").resolve("evidence");
        try (var entries = Files.newDirectoryStream(evidence, "mysql.dd.manifest.corrupt.*")) {
            assertTrue(entries.iterator().hasNext());
        }
        Files.delete(directory.resolve("mysql.ibd"));
        assertEquals(CatalogRecoveryStatus.REBUILDABLE,
                new CatalogRecoveryService(config()).inspect(Duration.ofSeconds(20)).status());
    }

    /**
     * 已存在的零长度 manifest 也是损坏证据，普通启动必须先原子保留，而不能把它当成“缺失的新文件”
     * 原地初始化；随后从有效 catalog/SDI 发布的新 clean snapshot 才能授权灾难重建。
     */
    @Test
    void healthyCatalogPreservesAndRegeneratesZeroLengthManifest() throws IOException {
        createHealthyTable();
        Path manifest = directory.resolve("mysql.dd.manifest");
        try (FileChannel channel = FileChannel.open(manifest, StandardOpenOption.WRITE)) {
            channel.truncate(0);
            channel.force(true);
        }

        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
        }

        Path evidence = directory.resolve("catalog-recovery").resolve("evidence");
        try (var entries = Files.newDirectoryStream(evidence, "mysql.dd.manifest.corrupt.*")) {
            assertTrue(entries.iterator().hasNext(),
                    "零长度 manifest 必须先保留为 evidence，不能原地覆盖");
        }
        Files.delete(directory.resolve("mysql.ibd"));
        assertEquals(CatalogRecoveryStatus.REBUILDABLE,
                new CatalogRecoveryService(config()).inspect(Duration.ofSeconds(20)).status());
    }

    /**
     * final catalog rename 失败时不得发布目标或删除已验证 temp；下一次 rebuild 应严格重开并复用 temp，
     * 从而覆盖“baseline durable、catalog 尚未发布”的 crash window。
     */
    @Test
    void retriesVerifiedTemporaryAfterFinalAtomicMoveFailure() throws IOException {
        createHealthyTable();
        Files.delete(directory.resolve("mysql.ibd"));
        Files.delete(directory.resolve("mysql.dd.ctrl"));
        CatalogRecoveryService inspectionService = new CatalogRecoveryService(config());
        CatalogRecoveryInspection inspection =
                inspectionService.inspect(Duration.ofSeconds(20));
        CatalogRecoveryToken token = inspection.token().orElseThrow();
        Path temporary = temporaryCatalog(token);
        CatalogRecoveryService failing = new CatalogRecoveryService(config(), (source, destination) -> {
            if (destination.equals(directory.resolve("mysql.ibd").toAbsolutePath().normalize())) {
                throw new IOException("injected final catalog rename failure");
            }
            Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE);
        });

        assertThrows(CatalogRecoveryException.class,
                () -> failing.rebuild(token, Duration.ofSeconds(20)));
        assertFalse(Files.exists(directory.resolve("mysql.ibd")));
        assertTrue(Files.isRegularFile(temporary));

        CatalogRecoveryInspection refreshed =
                new CatalogRecoveryService(config()).inspect(Duration.ofSeconds(20));
        CatalogRecoveryToken refreshedToken = refreshed.token().orElseThrow();
        assertNotEquals(token, refreshedToken,
                "第一次 rebuild 已 durable 创建 control，因此原始 complete-scan token 必须过期");
        assertEquals(temporary, temporaryCatalog(refreshedToken),
                "同一 clean baseline 必须跨 control 指纹变化复用固定临时文件");

        CatalogRebuildResult result = new CatalogRecoveryService(config()).rebuild(
                refreshedToken, Duration.ofSeconds(20));
        assertEquals(1, result.tableCount());
        assertTrue(Files.isRegularFile(directory.resolve("mysql.ibd")));
        assertFalse(Files.exists(temporary));
    }

    /**
     * 多文件 quarantine 在第二次 move 前失败时，第一项已经原子隔离的事实必须由下一次 inspect 接受；
     * 工具不能回滚为 copy/delete，也不能继续使用旧 token。
     */
    @Test
    void rescansAfterPartialQuarantineFailure() throws IOException {
        Path tablePath = createHealthyTable();
        Path catalog = directory.resolve("mysql.ibd");
        try (FileChannel channel = FileChannel.open(catalog, StandardOpenOption.WRITE)) {
            channel.truncate(0);
            channel.force(true);
        }
        Path extra = directory.resolve("tables").resolve("table_999_space_1999.ibd");
        Files.copy(tablePath, extra, StandardCopyOption.COPY_ATTRIBUTES);
        CatalogRecoveryInspection blocked =
                new CatalogRecoveryService(config()).inspect(Duration.ofSeconds(20));
        Set<CatalogRecoveryConflictId> selected = blocked.conflicts().stream()
                .filter(CatalogRecoveryConflict::quarantinable)
                .map(CatalogRecoveryConflict::id)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        java.util.concurrent.atomic.AtomicInteger moves = new java.util.concurrent.atomic.AtomicInteger();
        CatalogRecoveryService failing = new CatalogRecoveryService(config(), (source, destination) -> {
            if (moves.incrementAndGet() == 2) {
                throw new IOException("injected second quarantine move failure");
            }
            Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE);
        });

        assertThrows(CatalogRecoveryException.class, () -> failing.quarantine(
                blocked.token().orElseThrow(), selected, Duration.ofSeconds(20)));
        assertThrows(CatalogRecoveryStaleTokenException.class, () ->
                new CatalogRecoveryService(config()).quarantine(
                        blocked.token().orElseThrow(), selected, Duration.ofSeconds(20)));

        CatalogRecoveryInspection afterPartial =
                new CatalogRecoveryService(config()).inspect(Duration.ofSeconds(20));
        Set<CatalogRecoveryConflictId> remaining = afterPartial.conflicts().stream()
                .filter(CatalogRecoveryConflict::quarantinable)
                .map(CatalogRecoveryConflict::id)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        CatalogRecoveryInspection rebuildable = new CatalogRecoveryService(config()).quarantine(
                afterPartial.token().orElseThrow(), remaining, Duration.ofSeconds(20));
        assertEquals(CatalogRecoveryStatus.REBUILDABLE, rebuildable.status());
    }

    private Path createHealthyTable() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(1), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            return database.ddl().createTable(
                    MdlOwnerId.of(1),
                    new CreateTableCommand(
                            QualifiedTableName.of("app", "orders"),
                            PageNo.of(128),
                            List.of(new CreateColumnSpec(
                                    ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false))),
                            List.of(new CreateIndexSpec(
                                    ObjectName.of("PRIMARY"), true, true,
                                    List.of(new CreateIndexKeyPartSpec(
                                            ObjectName.of("id"), IndexOrder.ASC, 0))))),
                    Duration.ofSeconds(5)).storageBinding().orElseThrow().path();
        }
    }

    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    /**
     * 从文件起点覆盖固定长度零字节并 force，保留非零 EOF 以精确构造“非空但格式损坏”。
     *
     * @param path 已关闭生产 store 的 catalog 或 control 路径
     * @param length 要覆盖的前缀长度；不得超过测试文件物理长度
     */
    private static void overwritePrefixWithZeros(Path path, int length) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer zeros = ByteBuffer.allocate(length);
            while (zeros.hasRemaining()) {
                channel.write(zeros, zeros.position());
            }
            channel.force(true);
        }
    }

    /**
     * rebuild 临时文件按 clean manifest digest 命名；control/catalog 原始指纹变化不会改变该基线身份。
     *
     * @param token 已由完整 inspection 签发、携带 clean manifest digest 的恢复令牌
     * @return 与生产恢复服务相同的同目录临时 catalog 路径
     */
    private Path temporaryCatalog(CatalogRecoveryToken token) {
        return directory.resolve("mysql.ibd.rebuild." + token.manifestDigest().substring(0, 24));
    }
}
