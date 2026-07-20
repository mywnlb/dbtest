package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.repo.DictionaryControlSnapshot;
import cn.zhangyis.db.dd.repo.DictionarySnapshot;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.fil.catalog.FileDictionaryRecoveryManifestStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * manifest event TDD：验证 control 高水位、mutation fence、clean resolution 与跨重启恢复。
 */
class DictionaryRecoveryManifestRepositoryTest {

    @TempDir
    Path directory;

    /**
     * mutation intent 必须让旧 clean 失效；新 clean 只有在自身 committed 前记录的 resolvedThrough
     * 覆盖 intent 后才重新允许 rebuild，并能跨 store reopen 保持裁决。
     */
    @Test
    void fencesOldCleanUntilNewCleanResolvesMutationAcrossRestart() {
        Path path = directory.resolve("mysql.dd.manifest");
        DictionaryControlSnapshot initial = control(1, 2, 1, 1, 1024, 1, 2);
        DictionaryControlSnapshot reserved = control(2, 3, 4, 5, 1027, 8, 9);
        try (FileDictionaryRecoveryManifestStore store =
                     FileDictionaryRecoveryManifestStore.openOrCreate(path)) {
            DictionaryRecoveryManifestRepository manifest =
                    new DictionaryRecoveryManifestRepository(store);
            manifest.publishCleanSnapshot(DictionarySnapshot.emptyBootstrap(), initial,
                    directory.resolve("tables"));
            assertTrue(manifest.view().rebuildable());

            manifest.beforeControlReservation(reserved);
            manifest.beforeCatalogMutation(DictionaryVersion.of(2), List.of(
                    new CatalogRecord("mutation".getBytes(StandardCharsets.UTF_8),
                            "payload".getBytes(StandardCharsets.UTF_8))));
            assertTrue(manifest.view().unresolvedCatalogMutation());
            assertFalse(manifest.view().rebuildable());
            assertEquals(reserved, manifest.view().safeControl().orElseThrow());

            manifest.publishCleanSnapshot(DictionarySnapshot.emptyBootstrap(), reserved,
                    directory.resolve("tables"));
            assertTrue(manifest.view().rebuildable());
        }

        try (FileDictionaryRecoveryManifestStore store =
                     FileDictionaryRecoveryManifestStore.openExisting(path)) {
            DictionaryRecoveryManifestView reopened =
                    new DictionaryRecoveryManifestRepository(store).view();
            assertTrue(reopened.rebuildable());
            assertEquals(reserved, reopened.safeControl().orElseThrow());
            assertEquals(DictionarySnapshot.emptyBootstrap(),
                    reopened.latestClean().orElseThrow().dictionarySnapshot());
        }
    }

    /** 所有 reservation 逐分量取最大，不能因较新 event 的某个较小 counter 回退其它 identity。 */
    @Test
    void mergesControlReservationsByComponentMaximum() {
        DictionaryControlSnapshot left = control(5, 20, 10, 30, 1024, 40, 50);
        DictionaryControlSnapshot right = control(6, 12, 22, 25, 2048, 35, 60);

        DictionaryControlSnapshot maximum =
                DictionaryRecoveryManifestRepository.componentMaximum(left, right);

        assertEquals(20, maximum.nextSchemaId());
        assertEquals(22, maximum.nextTableId());
        assertEquals(30, maximum.nextIndexId());
        assertEquals(2048, maximum.nextSpaceId());
        assertEquals(40, maximum.nextDdlId());
        assertEquals(60, maximum.nextDictionaryVersion());
    }

    /** manifest v1 只见证 tables 根目录中的直接文件，嵌套相对路径不能逃离完整枚举范围。 */
    @Test
    void rejectsNestedRecoveryPath() {
        assertThrows(DatabaseValidationException.class, () ->
                new DictionaryRecoveryPathEntry(1, 1024, "nested/table_1_space_1024.ibd", new byte[32]));
    }

    /** 物理 batch 即使完整 durable，未知逻辑 event type 也必须在仓储重建时 fail-closed。 */
    @Test
    void rejectsCommittedUnknownLogicalEventType() {
        Path path = directory.resolve("mysql.dd.manifest");
        byte[] unknownChunkKey = ByteBuffer.allocate(13).order(ByteOrder.BIG_ENDIAN)
                .putInt(0x444D4331)
                .put((byte) 99)
                .putInt(0)
                .putInt(1)
                .array();
        try (FileDictionaryRecoveryManifestStore store =
                     FileDictionaryRecoveryManifestStore.openOrCreate(path)) {
            store.append(List.of(new CatalogRecord(unknownChunkKey, new byte[64])));

            assertThrows(DictionaryRecoveryManifestException.class,
                    () -> new DictionaryRecoveryManifestRepository(store));
        }
    }

    /** clean control 必须严格越过 archive 已用 identity/version，不能发布会导致重建后复用 ID 的快照。 */
    @Test
    void rejectsCleanSnapshotWhoseControlDoesNotCoverArchive() {
        Path path = directory.resolve("mysql.dd.manifest");
        SchemaDefinition schema = new SchemaDefinition(
                SchemaId.of(5), ObjectName.of("app"), 1, 1, DictionaryVersion.of(2));
        DictionarySnapshot snapshot = new DictionarySnapshot(
                DictionaryVersion.of(2), Map.of(schema.id(), schema), Map.of(), Map.of());
        DictionaryControlSnapshot tooLow = control(1, 5, 1, 1, 1024, 1, 2);
        try (FileDictionaryRecoveryManifestStore store =
                     FileDictionaryRecoveryManifestStore.openOrCreate(path)) {
            DictionaryRecoveryManifestRepository manifest =
                    new DictionaryRecoveryManifestRepository(store);

            assertThrows(DictionaryRecoveryManifestException.class,
                    () -> manifest.publishCleanSnapshot(snapshot, tooLow, directory.resolve("tables")));
            assertTrue(store.readCommittedBatches().isEmpty());
        }
    }

    private static DictionaryControlSnapshot control(
            long generation, long schema, long table, long index, long space, long ddl, long version) {
        return new DictionaryControlSnapshot(generation, SpaceId.of(1),
                schema, table, index, space, ddl, version);
    }
}
