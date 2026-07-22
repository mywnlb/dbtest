package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.ddl.DictionaryDdlLogStateException;
import cn.zhangyis.db.dd.ddl.DdlControlState;
import cn.zhangyis.db.dd.ddl.DdlDigestAlgorithm;
import cn.zhangyis.db.dd.ddl.DdlExecutionProtocol;
import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlLogPhase;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
import cn.zhangyis.db.dd.ddl.DdlSchemaCanonicalFormat;
import cn.zhangyis.db.dd.ddl.DdlSchemaDigest;
import cn.zhangyis.db.dd.domain.DdlId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DDL log repository TDD：钉死 marker/codec、operation-specific 阶段机、普通 DD batch 隔离与跨重启重建。
 */
class PersistentDdlLogRepositoryTest {

    @TempDir
    Path directory;

    /** CREATE/DROP 分别按自己的合法序列推进，终态和最大 ddl id 能从 durable batches 重建。 */
    @Test
    void persistsOperationSpecificTransitionsAcrossRestart() {
        Path catalog = directory.resolve("mysql.ibd");
        Path createPath = directory.resolve("tables/table_11_space_1024.ibd").toAbsolutePath().normalize();
        Path dropPath = directory.resolve("tables/table_12_space_1025.ibd").toAbsolutePath().normalize();
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            logs.prepare(productionRecord(
                    new DdlUndoMarker(7, 21, 11), DdlLogOperation.CREATE_TABLE, DdlLogPhase.PREPARED,
                    SpaceId.of(1024), createPath));
            logs.transition(DdlId.of(7), DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            logs.transition(DdlId.of(7), DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            logs.transition(DdlId.of(7), DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);

            logs.prepare(productionRecord(
                    new DdlUndoMarker(9, 22, 12), DdlLogOperation.DROP_TABLE, DdlLogPhase.PREPARED,
                    SpaceId.of(1025), dropPath));
            logs.transition(DdlId.of(9), DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            logs.transition(DdlId.of(9), DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            logs.transition(DdlId.of(9), DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            assertEquals(9, logs.highestDdlId());
            assertTrue(logs.unresolved().isEmpty());
        }

        try (FileInternalCatalogStore store = FileInternalCatalogStore.openExisting(catalog)) {
            PersistentDdlLogRepository reopened = new PersistentDdlLogRepository(store);
            assertEquals(DdlLogPhase.COMMITTED, reopened.find(DdlId.of(7)).orElseThrow().phase());
            assertEquals(DdlLogPhase.COMMITTED, reopened.find(DdlId.of(9)).orElseThrow().phase());
            assertEquals(9, reopened.highestDdlId());
        }
    }

    /**
     * CREATE INDEX marker 必须持久关联 table id 与新 index id，并使用 build→DD publish 的正向阶段机。
     */
    @Test
    void persistsCreateIndexIdentityAndTransitionsAcrossRestart() {
        Path catalog = directory.resolve("create-index-mysql.ibd");
        Path tablePath = directory.resolve("tables/table_11_space_1024.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            DdlLogRecord prepared = productionRecord(
                    new DdlUndoMarker(17, 31, 11), 23,
                    DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                    SpaceId.of(1024), tablePath);
            logs.prepare(prepared);
            logs.transition(DdlId.of(17), DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            logs.transition(DdlId.of(17), DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            logs.transition(DdlId.of(17), DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
        }

        try (FileInternalCatalogStore store = FileInternalCatalogStore.openExisting(catalog)) {
            DdlLogRecord recovered = new PersistentDdlLogRepository(store)
                    .find(DdlId.of(17)).orElseThrow();
            assertEquals(DdlLogOperation.CREATE_INDEX, recovered.operation());
            assertEquals(23, recovered.secondaryObjectId());
            assertEquals(DdlLogPhase.COMMITTED, recovered.phase());
        }
    }

    /** DROP INDEX 使用 dictionary-first 删除状态机，并在每个 durable phase 保持目标 secondary identity。 */
    @Test
    void persistsDropIndexIdentityAndTransitionsAcrossRestart() {
        Path catalog = directory.resolve("drop-index-mysql.ibd");
        Path tablePath = directory.resolve("tables/table_11_space_1024.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            logs.prepare(productionRecord(
                    new DdlUndoMarker(18, 32, 11), 23,
                    DdlLogOperation.DROP_INDEX, DdlLogPhase.PREPARED,
                    SpaceId.of(1024), tablePath));
            logs.transition(DdlId.of(18), DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            logs.transition(DdlId.of(18), DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            logs.transition(DdlId.of(18), DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
        }

        try (FileInternalCatalogStore store = FileInternalCatalogStore.openExisting(catalog)) {
            DdlLogRecord recovered = new PersistentDdlLogRepository(store)
                    .find(DdlId.of(18)).orElseThrow();
            assertEquals(DdlLogOperation.DROP_INDEX, recovered.operation());
            assertEquals(23, recovered.secondaryObjectId());
            assertEquals(DdlLogPhase.COMMITTED, recovered.phase());
        }
    }

    /**
     * 同一ALTER_TABLE_INPLACE operation按auxiliary journal区分instant与通用协议：通用路径必须先形成
     * ENGINE_DONE，旧instant marker仍禁止伪造物理完成阶段。
     */
    @Test
    void usesProtocolShapeAwareInplaceAlterPhaseGraph() {
        Path tablePath = directory.resolve("tables/table_41_space_3000.ibd");
        Path journal = directory.resolve("online-ddl/online-alter-301.log");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(
                directory.resolve("inplace-phase-mysql.ibd"))) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            DdlLogRecord general = new DdlLogRecord(
                    new DdlUndoMarker(301, 14, 41), 0,
                    DdlLogOperation.ALTER_TABLE_INPLACE, DdlLogPhase.PREPARED,
                    SpaceId.of(3000), tablePath, Optional.of(journal), Optional.empty(),
                    DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1,
                    Optional.of(digest(1)), Optional.empty(), Optional.of(digest(2)),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            logs.prepare(general);
            logs.compareAndSetControl(DdlId.of(301), DdlLogPhase.PREPARED,
                    DdlControlState.OPEN, DdlControlState.FORWARD_ONLY, Optional.empty());
            logs.transition(DdlId.of(301), DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            logs.transition(DdlId.of(301), DdlLogPhase.ENGINE_DONE,
                    DdlLogPhase.DICTIONARY_COMMITTED);
            logs.transition(DdlId.of(301), DdlLogPhase.DICTIONARY_COMMITTED,
                    DdlLogPhase.COMMITTED);

            DdlLogRecord instant = new DdlLogRecord(
                    new DdlUndoMarker(302, 15, 41), 0,
                    DdlLogOperation.ALTER_TABLE_INPLACE, DdlLogPhase.PREPARED,
                    SpaceId.of(3000), tablePath, Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1,
                    Optional.of(digest(1)), Optional.empty(), Optional.of(digest(2)),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            logs.prepare(instant);
            logs.compareAndSetControl(DdlId.of(302), DdlLogPhase.PREPARED,
                    DdlControlState.OPEN, DdlControlState.FORWARD_ONLY, Optional.empty());
            assertThrows(DictionaryDdlLogStateException.class,
                    () -> logs.transition(DdlId.of(302), DdlLogPhase.PREPARED,
                            DdlLogPhase.ENGINE_DONE));
            logs.transition(DdlId.of(302), DdlLogPhase.PREPARED,
                    DdlLogPhase.DICTIONARY_COMMITTED);
        }
    }

    /** 非法跳转、重复 prepare 与终态推进必须在 append 前拒绝，不能污染 durable phase history。 */
    @Test
    void rejectsInvalidTransitionsBeforePersistence() {
        try (FileInternalCatalogStore store =
                     FileInternalCatalogStore.openOrCreate(directory.resolve("invalid-mysql.ibd"))) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            DdlLogRecord prepared = productionRecord(
                    new DdlUndoMarker(3, 8, 31), DdlLogOperation.CREATE_TABLE, DdlLogPhase.PREPARED,
                    SpaceId.of(2048), directory.resolve("tables/table_31_space_2048.ibd"));
            logs.prepare(prepared);

            assertThrows(DictionaryDdlLogStateException.class,
                    () -> logs.prepare(prepared));
            assertThrows(DictionaryDdlLogStateException.class,
                    () -> logs.transition(DdlId.of(3), DdlLogPhase.PREPARED, DdlLogPhase.COMMITTED));
            logs.transition(DdlId.of(3), DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
            assertThrows(DictionaryDdlLogStateException.class,
                    () -> logs.transition(DdlId.of(3), DdlLogPhase.ROLLED_BACK, DdlLogPhase.PREPARED));
            assertEquals(2, store.readCommittedBatches().size());
        }
    }

    /** key/payload 双份 identity 必须稳定往返；任一侧被篡改都按 catalog corruption fail-closed。 */
    @Test
    void codecRejectsKeyPayloadIdentityMismatch() {
        DdlLogCatalogCodec codec = new DdlLogCatalogCodec();
        DdlLogRecord record = productionRecord(
                new DdlUndoMarker(5, 13, 41), DdlLogOperation.DROP_TABLE, DdlLogPhase.PREPARED,
                SpaceId.of(3000), directory.resolve("tables/table_41_space_3000.ibd"));
        CatalogRecord encoded = codec.encode(record).getFirst();
        DdlLogRecord decoded = codec.decode(new CatalogBatch(1, List.of(encoded))).orElseThrow();
        assertEquals(record, decoded);

        byte[] damagedPayload = encoded.payload();
        damagedPayload[8] ^= 0x01;
        assertThrows(cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException.class,
                () -> codec.decode(new CatalogBatch(1,
                        List.of(new CatalogRecord(encoded.key(), damagedPayload)))));
        assertArrayEquals(encoded.key(), codec.encode(decoded).getFirst().key());
    }

    /** 扩展前的 v1 table marker 没有 secondary identity，升级后必须继续可读且解码为 0。 */
    @Test
    void decodesLegacyV1TableMarkerWithoutSecondaryIdentity() {
        Path path = directory.resolve("tables/table_41_space_3000.ibd").toAbsolutePath().normalize();
        byte[] pathBytes = path.toString().getBytes(StandardCharsets.UTF_8);
        byte[] key = ByteBuffer.allocate(1 + Long.BYTES * 3 + Integer.BYTES * 2)
                .order(ByteOrder.BIG_ENDIAN)
                .put((byte) 7)
                .putLong(DdlLogOperation.CREATE_TABLE.stableCode())
                .putLong(5).putLong(13)
                .putInt(DdlLogPhase.PREPARED.stableCode()).putInt(0).array();
        byte[] payload = ByteBuffer.allocate(
                        Integer.BYTES + 1 + Long.BYTES * 3 + 1 + 1
                                + Integer.BYTES + Short.BYTES + pathBytes.length)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(0x44444C31).put((byte) 1)
                .putLong(5).putLong(13).putLong(41)
                .put((byte) DdlLogOperation.CREATE_TABLE.stableCode())
                .put((byte) DdlLogPhase.PREPARED.stableCode())
                .putInt(3000).putShort((short) pathBytes.length).put(pathBytes).array();

        DdlLogRecord decoded = new DdlLogCatalogCodec()
                .decode(new CatalogBatch(1, List.of(new CatalogRecord(key, payload)))).orElseThrow();

        assertEquals(DdlLogOperation.CREATE_TABLE, decoded.operation());
        assertEquals(0, decoded.secondaryObjectId());
        assertEquals(path, decoded.path());
    }

    /** v1 header、稳定码、单记录形状、路径长度与 UTF-8 必须全部 fail-closed，不能降级成 future batch。 */
    @Test
    void codecRejectsUnknownCodesAndMalformedPayloadShape() {
        DdlLogCatalogCodec codec = new DdlLogCatalogCodec();
        CatalogRecord encoded = codec.encode(productionRecord(
                new DdlUndoMarker(6, 14, 42), DdlLogOperation.CREATE_TABLE, DdlLogPhase.PREPARED,
                SpaceId.of(3001), directory.resolve("tables/table_42_space_3001.ibd"))).getFirst();

        byte[] unknownVersion = encoded.payload();
        unknownVersion[4] = 99;
        assertCatalogCorruption(codec, encoded.key(), unknownVersion);

        byte[] unknownOperationKey = encoded.key();
        ByteBuffer.wrap(unknownOperationKey).order(ByteOrder.BIG_ENDIAN).putLong(1, 99);
        byte[] unknownOperationPayload = encoded.payload();
        unknownOperationPayload[37] = 99;
        assertCatalogCorruption(codec, unknownOperationKey, unknownOperationPayload);

        byte[] unknownPhaseKey = encoded.key();
        ByteBuffer.wrap(unknownPhaseKey).order(ByteOrder.BIG_ENDIAN).putInt(33, 99);
        byte[] unknownPhasePayload = encoded.payload();
        unknownPhasePayload[38] = 99;
        assertCatalogCorruption(codec, unknownPhaseKey, unknownPhasePayload);

        byte[] trailing = Arrays.copyOf(encoded.payload(), encoded.payload().length + 1);
        assertCatalogCorruption(codec, encoded.key(), trailing);

        byte[] malformedUtf8 = encoded.payload();
        malformedUtf8[47] = (byte) 0xFF;
        assertCatalogCorruption(codec, encoded.key(), malformedUtf8);

        byte[] invalidPlatformPath = encoded.payload();
        invalidPlatformPath[47] = 0;
        assertCatalogCorruption(codec, encoded.key(), invalidPlatformPath);

        byte[] malformedDdlKey = Arrays.copyOf(encoded.key(), encoded.key().length - 1);
        assertThrows(cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException.class,
                () -> codec.decode(new CatalogBatch(1,
                        List.of(new CatalogRecord(malformedDdlKey, encoded.payload())))));
        assertThrows(cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException.class,
                () -> codec.decode(new CatalogBatch(1, List.of(encoded, encoded))));
    }

    /** v1 路径上限在编码前检查，越界 marker 不得进入 InternalCatalogStore。 */
    @Test
    void codecRejectsPathBeyondV1LimitBeforeAppend() {
        DdlLogCatalogCodec codec = new DdlLogCatalogCodec();
        DdlLogRecord tooLong = productionRecord(
                new DdlUndoMarker(8, 15, 43), DdlLogOperation.CREATE_TABLE, DdlLogPhase.PREPARED,
                SpaceId.of(3002), Path.of("x".repeat(901)));

        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> codec.encode(tooLong));
    }

    /** DDL reader 跳过普通/future batch；反过来 PersistentDictionaryRepository 也必须忽略 DDL marker batch。 */
    @Test
    void isolatesDdlLogBatchesFromDictionaryMutations() {
        try (FileInternalCatalogStore store =
                     FileInternalCatalogStore.openOrCreate(directory.resolve("isolated-mysql.ibd"))) {
            store.append(List.of(new CatalogRecord("future".getBytes(StandardCharsets.UTF_8),
                    "payload".getBytes(StandardCharsets.UTF_8))));
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            logs.prepare(productionRecord(
                    new DdlUndoMarker(4, 9, 51), DdlLogOperation.CREATE_TABLE, DdlLogPhase.PREPARED,
                    SpaceId.of(4096), directory.resolve("tables/table_51_space_4096.ibd")));

            assertEquals(1, logs.unresolved().size());
            assertEquals(1, new PersistentDictionaryRepository(store).snapshot().publishedVersion().value());
        }
    }

    private static void assertCatalogCorruption(DdlLogCatalogCodec codec, byte[] key, byte[] payload) {
        assertThrows(cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException.class,
                () -> codec.decode(new CatalogBatch(1, List.of(new CatalogRecord(key, payload)))));
    }

    /** 构造满足v4 checkpoint策略的表级production测试marker。 */
    private static DdlLogRecord productionRecord(
            DdlUndoMarker marker, DdlLogOperation operation, DdlLogPhase phase,
            SpaceId spaceId, Path path) {
        return productionRecord(marker, 0L, operation, phase, spaceId, path);
    }

    /** 构造满足v4 checkpoint策略的production测试marker，摘要内容只用于验证仓储协议。 */
    private static DdlLogRecord productionRecord(
            DdlUndoMarker marker, long secondaryObjectId, DdlLogOperation operation,
            DdlLogPhase phase, SpaceId spaceId, Path path) {
        Optional<DdlSchemaDigest> source = operation == DdlLogOperation.CREATE_TABLE
                ? Optional.empty() : Optional.of(digest(1));
        Optional<DdlSchemaDigest> intermediate = operation == DdlLogOperation.DROP_TABLE
                || operation == DdlLogOperation.DISCARD_TABLESPACE
                || operation == DdlLogOperation.IMPORT_TABLESPACE
                ? Optional.of(digest(2)) : Optional.empty();
        return new DdlLogRecord(marker, secondaryObjectId, operation, phase, spaceId, path,
                Optional.empty(), Optional.empty(), DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                source, intermediate, Optional.of(digest(3)), DdlControlState.OPEN,
                Optional.empty(), Optional.empty());
    }

    /** 生成长度正确且可稳定比较的SHA-256测试值。 */
    private static DdlSchemaDigest digest(int seed) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, (byte) seed);
        return new DdlSchemaDigest(DdlDigestAlgorithm.SHA_256,
                DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1, bytes);
    }
}
