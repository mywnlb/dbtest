package cn.zhangyis.db.storage.fil.catalog;

import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogCorruptionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * recovery manifest 独立物理 journal 的格式身份与跨重启测试。
 */
class FileDictionaryRecoveryManifestStoreTest {

    @TempDir
    Path directory;

    /** manifest batch 可严格重开，但其独立 magic 禁止同一文件被普通 catalog store 接受。 */
    @Test
    void persistsCommittedBatchAndRejectsCatalogMagicConfusion() {
        Path path = directory.resolve("mysql.dd.manifest");
        CatalogRecord record = new CatalogRecord(
                "event".getBytes(StandardCharsets.UTF_8),
                "body".getBytes(StandardCharsets.UTF_8));
        try (FileDictionaryRecoveryManifestStore store =
                     FileDictionaryRecoveryManifestStore.openOrCreate(path)) {
            store.append(List.of(record));
        }

        try (FileDictionaryRecoveryManifestStore store =
                     FileDictionaryRecoveryManifestStore.openExisting(path)) {
            List<CatalogRecord> reopened = store.readCommittedBatches().getFirst().records();
            assertEquals(1, reopened.size());
            assertArrayEquals(record.key(), reopened.getFirst().key());
            assertArrayEquals(record.payload(), reopened.getFirst().payload());
        }
        assertThrows(InternalCatalogCorruptionException.class,
                () -> FileInternalCatalogStore.openExisting(path));
    }

    /**
     * manifest 固定路径只接受 NOFOLLOW regular file。支持 symlink 的文件系统直接验证链接拒绝；
     * 不支持时退化为目录项测试，仍确保非常规路径不会进入 journal open/create。
     */
    @Test
    void rejectsSymbolicLinkOrOtherNonRegularManifestEntry() throws IOException {
        Path target = directory.resolve("manifest-target");
        try (FileDictionaryRecoveryManifestStore ignored =
                     FileDictionaryRecoveryManifestStore.openOrCreate(target)) {
            // 先建立合法目标，确保 symlink 分支不是因目标缺失失败。
        }
        Path fixedPath = directory.resolve("mysql.dd.manifest");
        try {
            Files.createSymbolicLink(fixedPath, target.getFileName());
        } catch (UnsupportedOperationException | IOException | SecurityException unsupported) {
            Files.createDirectory(fixedPath);
        }

        assertThrows(InternalCatalogCorruptionException.class,
                () -> FileDictionaryRecoveryManifestStore.openExisting(fixedPath));
        assertThrows(InternalCatalogCorruptionException.class,
                () -> FileDictionaryRecoveryManifestStore.openOrCreate(fixedPath));
    }

    /**
     * openOrCreate 只能创建明确缺失的 manifest；已存在的零长度证据必须严格失败且保持原长度，
     * 由持有健康 catalog 的上层决定是否原子移入 evidence。
     */
    @Test
    void preservesExistingZeroLengthManifestInsteadOfInitializingIt() throws IOException {
        Path path = directory.resolve("mysql.dd.manifest");
        Files.write(path, new byte[0], StandardOpenOption.CREATE_NEW);

        assertThrows(InternalCatalogCorruptionException.class,
                () -> FileDictionaryRecoveryManifestStore.openOrCreate(path));
        assertEquals(0, Files.size(path));
    }
}
