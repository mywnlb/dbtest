package cn.zhangyis.db.engine;

import cn.zhangyis.db.dd.exception.DictionaryCatalogAdmissionException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogCorruptionException;
import cn.zhangyis.db.storage.engine.EngineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Catalog 启动准入测试：只有完全没有权威持久痕迹的目录才允许创建空 catalog。
 *
 * <p>测试直接覆盖 package-private guard，以便逐类固定证据边界；公共组合根的资源和状态语义由
 * {@link DatabaseEngineTest} 覆盖。</p>
 */
class CatalogBootstrapAdmissionTest {

    /** 每个测试拥有独立临时根目录，失败用例不得在目录外产生文件副作用。 */
    @TempDir
    Path directory;

    /**
     * 完全空白的实例目录必须被识别为 fresh，并生成可由 catalog store 正常关闭的双 header 文件。
     */
    @Test
    void initializesCatalogWhenNoAuthoritativeArtifactsExist() throws Exception {
        Path baseDir = directory.resolve("fresh");
        Files.createDirectories(baseDir);

        try (var ignored = openCatalog(baseDir)) {
            assertTrue(Files.size(baseDir.resolve("mysql.ibd")) > 0);
        }
    }

    /**
     * 首次启动中断可能留下零长度占位文件；没有其它持久证据时仍允许完成 catalog 初始化。
     */
    @Test
    void initializesZeroLengthCatalogWhenNoAuthoritativeArtifactsExist() throws Exception {
        Path baseDir = directory.resolve("empty-catalog");
        Files.createDirectories(baseDir);
        Files.createFile(baseDir.resolve("mysql.ibd"));

        try (var ignored = openCatalog(baseDir)) {
            assertTrue(Files.size(baseDir.resolve("mysql.ibd")) > 0);
        }
    }

    /**
     * 逐项验证严格证据矩阵。每个子目录只放一种证据，避免某个强证据遮蔽另一类漏检。
     */
    @Test
    void rejectsMissingCatalogForEveryAuthoritativeArtifactKind() throws Exception {
        List<ArtifactCase> cases = List.of(
                new ArtifactCase("dictionary-control", "mysql.dd.ctrl", "DICTIONARY_CONTROL"),
                new ArtifactCase("dictionary-recovery-manifest", "mysql.dd.manifest",
                        "DICTIONARY_RECOVERY_MANIFEST"),
                new ArtifactCase("single-redo", "redo.log", "SINGLE_FILE_REDO"),
                new ArtifactCase("partial-ring", "redo/redo-000000.log", "REDO_RING"),
                new ArtifactCase("redo-control", "redo-control", "REDO_CONTROL"),
                new ArtifactCase("transaction-control", "transaction-recovery-control",
                        "TRANSACTION_RECOVERY_CONTROL"),
                new ArtifactCase("configured-undo", "undo_5.ibu", "UNDO_TABLESPACE"),
                new ArtifactCase("other-undo", "undo_999.ibu", "UNDO_TABLESPACE"),
                new ArtifactCase("legacy-doublewrite", "doublewrite.dwb", "DOUBLEWRITE"),
                new ArtifactCase("flush-list-doublewrite", "doublewrite-flush-list.dwb", "DOUBLEWRITE"),
                new ArtifactCase("lru-doublewrite", "doublewrite-lru.dwb", "DOUBLEWRITE"),
                new ArtifactCase("controlled-tablespace", "tables/table_1_space_1024.ibd",
                        "CONTROLLED_TABLESPACE"));

        for (ArtifactCase artifactCase : cases) {
            Path baseDir = directory.resolve(artifactCase.caseName());
            Path artifact = baseDir.resolve(artifactCase.relativePath());
            Files.createDirectories(artifact.getParent());
            byte[] original = new byte[]{0x31, 0x32, 0x33};
            Files.write(artifact, original);

            DictionaryCatalogAdmissionException failure = assertThrows(
                    DictionaryCatalogAdmissionException.class, () -> openCatalog(baseDir),
                    "artifact must reject fresh catalog bootstrap: " + artifactCase.relativePath());

            assertTrue(failure.getMessage().contains(artifactCase.expectedEvidence()),
                    "diagnostic must name the evidence category");
            assertFalse(Files.exists(baseDir.resolve("mysql.ibd")),
                    "admission failure must not create an empty catalog");
            assertArrayEquals(original, Files.readAllBytes(artifact),
                    "admission scan must not mutate recovery evidence");
        }
    }

    /**
     * 零长度 catalog 与 existing 证据组合不能被当成首次启动；guard 必须保留空文件供诊断。
     */
    @Test
    void rejectsZeroLengthCatalogWhenAuthoritativeArtifactExists() throws Exception {
        Path baseDir = directory.resolve("zero-with-control");
        Files.createDirectories(baseDir);
        Path catalog = Files.createFile(baseDir.resolve("mysql.ibd"));
        Files.write(baseDir.resolve("mysql.dd.ctrl"), new byte[]{0x01});

        assertThrows(DictionaryCatalogAdmissionException.class, () -> openCatalog(baseDir));

        assertEquals(0, Files.size(catalog));
    }

    /**
     * progress、warmup 与近似文件名不能制造 existing 假阳性，否则人工诊断文件会阻断真正的首次建库。
     */
    @Test
    void ignoresNonAuthoritativeDiagnosticAndNearMissFiles() throws Exception {
        Path baseDir = directory.resolve("diagnostic-only");
        Files.createDirectories(baseDir.resolve("redo"));
        Files.createDirectories(baseDir.resolve("tables"));
        Files.write(baseDir.resolve("recovery-progress.jsonl"), new byte[]{0x01});
        Files.write(baseDir.resolve("buffer-pool.dump"), new byte[]{0x02});
        Files.write(baseDir.resolve("notes.txt"), new byte[]{0x03});
        Files.write(baseDir.resolve("redo/redo-diagnostic.log"), new byte[]{0x04});
        Files.write(baseDir.resolve("undo_backup.ibu"), new byte[]{0x05});
        Files.write(baseDir.resolve("tables/unmanaged.ibd"), new byte[]{0x06});

        try (var ignored = openCatalog(baseDir)) {
            assertTrue(Files.size(baseDir.resolve("mysql.ibd")) > 0);
        }
    }

    /**
     * 非空 catalog 永远走 existing 格式校验；即使没有其它文件，损坏内容也不得被空 catalog 覆盖。
     */
    @Test
    void validatesNonEmptyCatalogInsteadOfReinitializingIt() throws Exception {
        Path baseDir = directory.resolve("corrupt-existing");
        Files.createDirectories(baseDir);
        Path catalog = baseDir.resolve("mysql.ibd");
        byte[] corrupt = new byte[]{0x11, 0x22, 0x33};
        Files.write(catalog, corrupt);

        assertThrows(InternalCatalogCorruptionException.class, () -> openCatalog(baseDir));

        assertArrayEquals(corrupt, Files.readAllBytes(catalog));
    }

    /**
     * 候选目录存在但无法按目录扫描时，guard 不能把“未知”解释成“没有证据”，并必须保留 IO 根因。
     */
    @Test
    void failsClosedWhenPersistentEvidenceCannotBeInspected() throws Exception {
        Path baseDir = directory.resolve("unreadable-shape");
        Files.createDirectories(baseDir);
        Files.write(baseDir.resolve("tables"), new byte[]{0x01});

        DictionaryCatalogAdmissionException failure =
                assertThrows(DictionaryCatalogAdmissionException.class, () -> openCatalog(baseDir));

        assertNotNull(failure.getCause());
        assertFalse(Files.exists(baseDir.resolve("mysql.ibd")));
    }

    /**
     * 用与公共组合根一致的固定路径调用准入 guard。
     *
     * @param baseDir 当前测试实例的独占根目录
     * @return 已打开的 catalog store；调用方必须通过 try-with-resources 关闭
     */
    private cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore openCatalog(Path baseDir) {
        EngineConfig config = config(baseDir);
        return CatalogBootstrapAdmission.openCatalog(config,
                baseDir.resolve("mysql.ibd"),
                baseDir.resolve("mysql.dd.ctrl"),
                baseDir.resolve("tables"));
    }

    /**
     * 构造与公共引擎默认 redo ring 布局一致的最小测试配置。
     *
     * @param baseDir 测试独占实例目录
     * @return 已完成领域校验的不可变引擎配置
     */
    private static EngineConfig config(Path baseDir) {
        return new EngineConfig(baseDir, PageSize.ofBytes(16 * 1024), 64,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(5), 64L * 1024 * 1024);
    }

    /**
     * 单一证据测试向量；relativePath 是实际文件布局，expectedEvidence 是稳定诊断分类。
     *
     * @param caseName 子实例目录名，保证各证据互不遮蔽
     * @param relativePath 相对实例根目录的持久证据路径
     * @param expectedEvidence 失败消息必须携带的证据分类
     */
    private record ArtifactCase(String caseName, String relativePath, String expectedEvidence) {
    }
}
