package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.ddl.DdlCancellation;
import cn.zhangyis.db.dd.ddl.DdlCancellationReason;
import cn.zhangyis.db.dd.ddl.DdlBatchManifest;
import cn.zhangyis.db.dd.ddl.DdlBatchSchemaEntry;
import cn.zhangyis.db.dd.ddl.DdlBatchTableEntry;
import cn.zhangyis.db.dd.ddl.DdlControlCasResult;
import cn.zhangyis.db.dd.ddl.DdlControlState;
import cn.zhangyis.db.dd.ddl.DdlDigestAlgorithm;
import cn.zhangyis.db.dd.ddl.DdlExecutionProtocol;
import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlLogPhase;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
import cn.zhangyis.db.dd.ddl.DdlRetiredResource;
import cn.zhangyis.db.dd.ddl.DdlRetiredResourceKind;
import cn.zhangyis.db.dd.ddl.DdlRetirementFence;
import cn.zhangyis.db.dd.ddl.DdlSchemaCanonicalFormat;
import cn.zhangyis.db.dd.ddl.DdlSchemaDigest;
import cn.zhangyis.db.dd.domain.DdlId;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DDL marker v4/v5 codec、legacy 兼容、batch manifest 与 durable control CAS 的协议测试。 */
class DdlLogV4Test {

    @TempDir
    Path directory;

    /**
     * v5 必须把排序后的多表 checkpoint 与 schema 证据放入同一分块 batch，
     * phase 推进和重启解码均不得丢失或重排 manifest。
     */
    @Test
    void roundTripsChunkedV5BatchDropManifest() {
        ArrayList<DdlBatchTableEntry> tables =
                new ArrayList<>();
        for (long tableId = 60; tableId >= 41; tableId--) {
            int spaceId = 3000 + Math.toIntExact(tableId - 40);
            tables.add(new DdlBatchTableEntry(
                    TableId.of(tableId), SpaceId.of(spaceId),
                    "table_" + tableId + "_space_" + spaceId
                            + ".ibd",
                    2, digest(1), digest(2), digest(3)));
        }
        DdlBatchManifest manifest = new DdlBatchManifest(
                Optional.of(new DdlBatchSchemaEntry(
                        SchemaId.of(9), "app",
                        schemaDigest(11), schemaDigest(12))),
                tables);
        DdlLogRecord prepared = new DdlLogRecord(
                new DdlUndoMarker(701, 30, 9), 0,
                DdlLogOperation.DROP_SCHEMA_CASCADE,
                DdlLogPhase.PREPARED, SpaceId.of(3001),
                directory.resolve("table_41_space_3001.ibd"),
                Optional.empty(), Optional.empty(),
                DdlExecutionProtocol.BATCH_DROP_V1,
                Optional.empty(), Optional.empty(), Optional.empty(),
                DdlControlState.OPEN, Optional.empty(), Optional.empty(),
                Optional.of(manifest));

        DdlLogCatalogCodec codec = new DdlLogCatalogCodec();
        List<CatalogRecord> encoded = codec.encode(prepared);
        DdlLogRecord decoded = codec.decode(
                new CatalogBatch(1, encoded)).orElseThrow();

        assertTrue(encoded.size() > 1);
        assertEquals(41L, decoded.batchManifest().orElseThrow()
                .tables().getFirst().tableId().value());
        assertEquals(60L, decoded.batchManifest().orElseThrow()
                .tables().getLast().tableId().value());
        assertEquals(prepared, decoded);
        assertEquals(prepared.batchManifest(),
                decoded.withPhase(
                        DdlLogPhase.DICTIONARY_COMMITTED)
                        .batchManifest());
    }

    /** encoder 与 decoder 必须共享 4096 表上限，禁止写出当前版本自身无法重开的 marker。 */
    @Test
    void rejectsV5BatchManifestAboveDecodeBound() {
        ArrayList<DdlBatchTableEntry> tables =
                new ArrayList<>(DdlBatchManifest.MAX_TABLES + 1);
        for (int identity = 1;
             identity <= DdlBatchManifest.MAX_TABLES + 1;
             identity++) {
            tables.add(new DdlBatchTableEntry(
                    TableId.of(identity), SpaceId.of(identity),
                    "table_" + identity + "_space_"
                            + identity + ".ibd",
                    1, digest(1), digest(2), digest(3)));
        }

        assertThrows(
                cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> new DdlBatchManifest(
                        Optional.empty(), tables));
    }

    /** 两条长路径和完整v4字段必须通过同一catalog batch分块，重启后逐字段恢复。 */
    @Test
    void roundTripsChunkedV4RecordWithAllFields() {
        Path primary = directory.resolve("p".repeat(700) + ".ibd");
        Path auxiliary = directory.resolve("a".repeat(700) + ".rowlog");
        DdlRetirementFence fence = new DdlRetirementFence(
                41, 12, 99, 12, 3, 77,
                List.of(new DdlRetiredResource(DdlRetiredResourceKind.INDEX, 8),
                        new DdlRetiredResource(DdlRetiredResourceKind.INDEX, 9)));
        DdlLogRecord record = new DdlLogRecord(
                new DdlUndoMarker(77, 13, 41), 0,
                DdlLogOperation.ALTER_TABLE_INPLACE, DdlLogPhase.PREPARED,
                SpaceId.of(3000), primary, Optional.of(auxiliary),
                Optional.of(new TablespaceFileIdentity(
                        SpaceId.of(3000), PageSize.ofBytes(16 * 1024), TablespaceType.GENERAL, 1, 5)),
                DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1,
                Optional.of(digest(1)), Optional.empty(), Optional.of(digest(2)),
                DdlControlState.CANCEL_REQUESTED,
                Optional.of(new DdlCancellation(DdlCancellationReason.USER_REQUEST, 1234, 55)),
                Optional.of(fence));

        List<CatalogRecord> encoded = new DdlLogCatalogCodec().encode(record);
        DdlLogRecord decoded = new DdlLogCatalogCodec()
                .decode(new CatalogBatch(1, encoded)).orElseThrow();

        assertTrue(encoded.size() > 1);
        assertTrue(encoded.stream().allMatch(chunk -> chunk.payload().length <= 1024));
        assertEquals(record, decoded);
    }

    /** retirement fence不能挂到Online ADD；否则重放可能把无关旧资源误解释为ADD的清理责任。 */
    @Test
    void rejectsRetirementFenceOnProtocolWithoutRetirementSemantics() {
        DdlRetirementFence fence = new DdlRetirementFence(
                41, 12, 99, 12, 3, 77,
                List.of(new DdlRetiredResource(DdlRetiredResourceKind.INDEX, 8)));

        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> new DdlLogRecord(
                        new DdlUndoMarker(77, 13, 41), 8,
                        DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                        SpaceId.of(3000), directory.resolve("table.ibd"),
                        Optional.of(directory.resolve("table.rowlog")), Optional.empty(),
                        DdlExecutionProtocol.ONLINE_INDEX_V1,
                        Optional.of(digest(1)), Optional.empty(), Optional.of(digest(2)),
                        DdlControlState.OPEN, Optional.empty(), Optional.of(fence)));
    }

    /** retirement fence资源集合属于持久协议输入；null元素必须映射为稳定领域异常，不能泄漏JDK异常。 */
    @Test
    void rejectsNullRetirementResourceWithDomainException() {
        List<DdlRetiredResource> resources = new ArrayList<>();
        resources.add(null);

        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> new DdlRetirementFence(41, 12, 99, 12, 3, 77, resources));
    }

    /** v4 chunks必须从0连续且保持batch顺序；交换、缺失或重复均不能形成部分marker。 */
    @Test
    void rejectsMissingReorderedAndDuplicateV4Chunks() {
        DdlLogCatalogCodec codec = new DdlLogCatalogCodec();
        List<CatalogRecord> encoded = codec.encode(onlinePrepared(81,
                directory.resolve("p".repeat(700) + ".ibd"),
                Optional.of(directory.resolve("a".repeat(700) + ".rowlog"))));
        assertTrue(encoded.size() > 1);

        List<CatalogRecord> reordered = new ArrayList<>(encoded);
        CatalogRecord first = reordered.get(0);
        reordered.set(0, reordered.get(1));
        reordered.set(1, first);
        assertThrows(cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException.class,
                () -> codec.decode(new CatalogBatch(1, reordered)));

        assertThrows(cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException.class,
                () -> codec.decode(new CatalogBatch(1, encoded.subList(0, encoded.size() - 1))));

        List<CatalogRecord> duplicated = new ArrayList<>(encoded);
        duplicated.add(1, encoded.get(0));
        assertThrows(cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException.class,
                () -> codec.decode(new CatalogBatch(1, duplicated)));
    }

    /** 真实v2 payload包含secondary identity；升级不能把它错读为operation/phase或降级成表级marker。 */
    @Test
    void decodesRealV2SecondaryIdentityFixture() {
        Path path = directory.resolve("tables/table_41_space_3000.ibd").toAbsolutePath().normalize();
        byte[] pathBytes = path.toString().getBytes(StandardCharsets.UTF_8);
        byte[] key = keyV2(DdlLogOperation.CREATE_INDEX, 5, 13, 23,
                DdlLogPhase.PREPARED, 0);
        byte[] payload = ByteBuffer.allocate(Integer.BYTES + 1 + Long.BYTES * 4 + 1 + 1
                        + Integer.BYTES + Short.BYTES + pathBytes.length)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(0x44444C31).put((byte) 2)
                .putLong(5).putLong(13).putLong(41).putLong(23)
                .put((byte) DdlLogOperation.CREATE_INDEX.stableCode())
                .put((byte) DdlLogPhase.PREPARED.stableCode())
                .putInt(3000).putShort((short) pathBytes.length).put(pathBytes).array();

        DdlLogRecord decoded = new DdlLogCatalogCodec()
                .decode(new CatalogBatch(1, List.of(new CatalogRecord(key, payload)))).orElseThrow();

        assertEquals(23, decoded.secondaryObjectId());
        assertEquals(DdlExecutionProtocol.LEGACY_PHASE_ONLY, decoded.executionProtocol());
        assertTrue(decoded.sourceSchemaDigest().isEmpty());
        assertTrue(decoded.targetSchemaDigest().isEmpty());
    }

    /** phase推进只能替换phase；protocol、三个digest、control、cancel和fence都必须原样保留。 */
    @Test
    void phaseTransitionPreservesEveryV4Field() {
        Path catalog = directory.resolve("phase-v4-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            DdlLogRecord prepared = onlinePrepared(91, directory.resolve("table.ibd"),
                    Optional.of(directory.resolve("table.rowlog")));
            logs.prepare(prepared);
            DdlControlCasResult fenced = logs.compareAndSetControl(
                    DdlId.of(91), DdlLogPhase.PREPARED, DdlControlState.OPEN,
                    DdlControlState.FORWARD_ONLY, Optional.empty());
            DdlLogRecord advanced = logs.transition(
                    DdlId.of(91), DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);

            assertTrue(fenced.changed());
            assertEquals(prepared.sourceSchemaDigest(), advanced.sourceSchemaDigest());
            assertEquals(prepared.targetSchemaDigest(), advanced.targetSchemaDigest());
            assertEquals(DdlExecutionProtocol.ONLINE_INDEX_V1, advanced.executionProtocol());
            assertEquals(DdlControlState.FORWARD_ONLY, advanced.controlState());
            assertTrue(advanced.cancellation().isEmpty());
        }
    }

    /** cancel与forward在同一writer fence下竞争1000次，每次只能有一个durable方向。 */
    @Test
    void letsExactlyOneCancelOrForwardControlCasWin() throws Exception {
        Path catalog = directory.resolve("control-race-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            for (int iteration = 1; iteration <= 1000; iteration++) {
                int attempt = iteration;
                long ddlId = 1000L + iteration;
                logs.prepare(onlinePrepared(ddlId, directory.resolve("table-" + ddlId + ".ibd"),
                        Optional.of(directory.resolve("table-" + ddlId + ".rowlog"))));
                CountDownLatch start = new CountDownLatch(1);
                Future<DdlControlCasResult> cancel = executor.submit(() -> {
                    start.await();
                    return logs.compareAndSetControl(DdlId.of(ddlId), DdlLogPhase.PREPARED,
                            DdlControlState.OPEN, DdlControlState.CANCEL_REQUESTED,
                            Optional.of(new DdlCancellation(
                                    DdlCancellationReason.USER_REQUEST, attempt, 7)));
                });
                Future<DdlControlCasResult> forward = executor.submit(() -> {
                    start.await();
                    return logs.compareAndSetControl(DdlId.of(ddlId), DdlLogPhase.PREPARED,
                            DdlControlState.OPEN, DdlControlState.FORWARD_ONLY, Optional.empty());
                });
                start.countDown();

                DdlControlCasResult cancelResult = cancel.get();
                DdlControlCasResult forwardResult = forward.get();
                assertEquals(1, (cancelResult.changed() ? 1 : 0) + (forwardResult.changed() ? 1 : 0));
                DdlControlState durable = logs.find(DdlId.of(ddlId)).orElseThrow().controlState();
                assertTrue(durable == DdlControlState.CANCEL_REQUESTED
                        || durable == DdlControlState.FORWARD_ONLY);
                assertEquals(durable, cancelResult.changed()
                        ? cancelResult.observedRecord().controlState()
                        : forwardResult.observedRecord().controlState());
            }
        }
    }

    /** cancel胜出后只能回滚，forward胜出后只能前滚；blocking/legacy marker均不得进入cancel CAS。 */
    @Test
    void enforcesControlPhaseCrossProductAndProtocolCapability() {
        Path catalog = directory.resolve("control-matrix-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            logs.prepare(onlinePrepared(201, directory.resolve("cancel.ibd"),
                    Optional.of(directory.resolve("cancel.rowlog"))));
            logs.compareAndSetControl(DdlId.of(201), DdlLogPhase.PREPARED, DdlControlState.OPEN,
                    DdlControlState.CANCEL_REQUESTED,
                    Optional.of(new DdlCancellation(DdlCancellationReason.USER_REQUEST, 1, 1)));
            assertThrows(cn.zhangyis.db.dd.ddl.DictionaryDdlLogStateException.class,
                    () -> logs.transition(DdlId.of(201), DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE));
            assertEquals(DdlLogPhase.ROLLED_BACK, logs.transition(
                    DdlId.of(201), DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK).phase());

            logs.prepare(onlinePrepared(202, directory.resolve("forward.ibd"),
                    Optional.of(directory.resolve("forward.rowlog"))));
            logs.compareAndSetControl(DdlId.of(202), DdlLogPhase.PREPARED, DdlControlState.OPEN,
                    DdlControlState.FORWARD_ONLY, Optional.empty());
            assertThrows(cn.zhangyis.db.dd.ddl.DictionaryDdlLogStateException.class,
                    () -> logs.transition(DdlId.of(202), DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK));

            DdlLogRecord blocking = new DdlLogRecord(
                    new DdlUndoMarker(203, 14, 41), 8,
                    DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                    SpaceId.of(3000), directory.resolve("blocking.ibd"), Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(digest(1)), Optional.empty(), Optional.of(digest(2)),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            logs.prepare(blocking);
            assertThrows(cn.zhangyis.db.dd.ddl.DictionaryDdlLogStateException.class,
                    () -> logs.compareAndSetControl(DdlId.of(203), DdlLogPhase.PREPARED,
                            DdlControlState.OPEN, DdlControlState.CANCEL_REQUESTED,
                            Optional.of(new DdlCancellation(DdlCancellationReason.USER_REQUEST, 1, 1))));
        }
    }

    /** retirement fence必须在PREPARED/OPEN冻结；取消或前滚方向一旦持久化就不能反向补写边界。 */
    @Test
    void rejectsInstallingRetirementFenceAfterControlDirectionIsChosen() {
        Path catalog = directory.resolve("late-retirement-fence-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            logs.prepare(onlineDropPrepared(211, directory.resolve("cancel-drop.ibd")));
            logs.compareAndSetControl(DdlId.of(211), DdlLogPhase.PREPARED, DdlControlState.OPEN,
                    DdlControlState.CANCEL_REQUESTED,
                    Optional.of(new DdlCancellation(DdlCancellationReason.USER_REQUEST, 1, 1)));

            assertThrows(cn.zhangyis.db.dd.ddl.DictionaryDdlLogStateException.class,
                    () -> logs.installRetirementFence(
                            DdlId.of(211), DdlLogPhase.PREPARED,
                            DdlControlState.CANCEL_REQUESTED, retirementFence(211)));

            logs.prepare(onlineDropPrepared(212, directory.resolve("forward-drop.ibd")));
            logs.compareAndSetControl(DdlId.of(212), DdlLogPhase.PREPARED, DdlControlState.OPEN,
                    DdlControlState.FORWARD_ONLY, Optional.empty());
            assertThrows(cn.zhangyis.db.dd.ddl.DictionaryDdlLogStateException.class,
                    () -> logs.installRetirementFence(
                            DdlId.of(212), DdlLogPhase.PREPARED,
                            DdlControlState.FORWARD_ONLY, retirementFence(212)));
        }
    }

    /** startup必须拒绝“先FORWARD_ONLY、后fence”的反序历史，不能把未知cutover边界解释成可信证据。 */
    @Test
    void rejectsRetirementFenceAppendedAfterForwardFenceWhenRebuildingHistory() {
        Path catalog = directory.resolve("reordered-retirement-fence-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            DdlLogCatalogCodec codec = new DdlLogCatalogCodec();
            DdlLogRecord prepared = onlineDropPrepared(213, directory.resolve("drop.ibd"));
            DdlLogRecord forward = prepared.withControl(DdlControlState.FORWARD_ONLY, Optional.empty());
            store.append(codec.encode(prepared));
            store.append(codec.encode(forward));
            store.append(codec.encode(forward.withRetirementFence(retirementFence(213))));

            assertThrows(cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException.class,
                    () -> new PersistentDdlLogRepository(store));
        }
    }

    /** 新production prepare必须满足逐operation checkpoint策略，legacy空digest构造不能继续写入新marker。 */
    @Test
    void rejectsNewMarkerThatOmitsRequiredSchemaDigests() {
        Path catalog = directory.resolve("digest-policy-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            PersistentDdlLogRepository logs = new PersistentDdlLogRepository(store);
            DdlLogRecord invalid = new DdlLogRecord(
                    new DdlUndoMarker(301, 13, 41), 8,
                    DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                    SpaceId.of(3000), directory.resolve("invalid.ibd"), Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.empty(), Optional.empty(), Optional.empty(),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());

            assertThrows(cn.zhangyis.db.dd.ddl.DictionaryDdlLogStateException.class,
                    () -> logs.prepare(invalid));
        }
    }

    /** encoder不得把仅供v1-v3 decoder产生的LEGACY protocol写入v4，否则新历史可绕过digest门禁。 */
    @Test
    void rejectsEncodingLegacyProtocolAsV4() {
        DdlLogRecord legacy = new DdlLogRecord(
                new DdlUndoMarker(302, 13, 41), 8,
                DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                SpaceId.of(3000), directory.resolve("legacy.ibd"));

        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> new DdlLogCatalogCodec().encode(legacy));
    }

    /** startup rebuild必须复用live prepare的v4语义校验，不能接受缺digest的首条PREPARED。 */
    @Test
    void rejectsInvalidInitialV4MarkerWhenRebuildingHistory() {
        Path catalog = directory.resolve("invalid-initial-v4-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            DdlLogRecord invalid = new DdlLogRecord(
                    new DdlUndoMarker(303, 13, 41), 8,
                    DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                    SpaceId.of(3000), directory.resolve("invalid-initial.ibd"),
                    Optional.empty(), Optional.empty(), DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.empty(), Optional.empty(), Optional.empty(),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            store.append(new DdlLogCatalogCodec().encode(invalid));

            assertThrows(cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException.class,
                    () -> new PersistentDdlLogRepository(store));
        }
    }

    private DdlLogRecord onlinePrepared(long ddlId, Path path, Optional<Path> auxiliary) {
        return new DdlLogRecord(
                new DdlUndoMarker(ddlId, 13, 41), 8,
                DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                SpaceId.of(3000), path, auxiliary, Optional.empty(),
                DdlExecutionProtocol.ONLINE_INDEX_V1,
                Optional.of(digest(1)), Optional.empty(), Optional.of(digest(2)),
                DdlControlState.OPEN, Optional.empty(), Optional.empty());
    }

    private DdlLogRecord onlineDropPrepared(long ddlId, Path path) {
        return new DdlLogRecord(
                new DdlUndoMarker(ddlId, 13, 41), 8,
                DdlLogOperation.DROP_INDEX, DdlLogPhase.PREPARED,
                SpaceId.of(3000), path, Optional.empty(), Optional.empty(),
                DdlExecutionProtocol.ONLINE_DROP_INDEX_V1,
                Optional.of(digest(1)), Optional.empty(), Optional.of(digest(2)),
                DdlControlState.OPEN, Optional.empty(), Optional.empty());
    }

    private static DdlRetirementFence retirementFence(long ddlId) {
        return new DdlRetirementFence(
                41, 12, 99, 12, 13, ddlId,
                List.of(new DdlRetiredResource(DdlRetiredResourceKind.INDEX, 8)));
    }

    private static DdlSchemaDigest digest(int seed) {
        byte[] bytes = new byte[32];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) (seed + index);
        }
        return new DdlSchemaDigest(DdlDigestAlgorithm.SHA_256,
                DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1, bytes);
    }

    private static DdlSchemaDigest schemaDigest(int seed) {
        byte[] bytes = new byte[32];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) (seed + index);
        }
        return new DdlSchemaDigest(
                DdlDigestAlgorithm.SHA_256,
                DdlSchemaCanonicalFormat.SCHEMA_V1, bytes);
    }

    private static byte[] keyV2(DdlLogOperation operation, long ddlId, long version,
                                long secondaryObjectId, DdlLogPhase phase, int chunk) {
        return ByteBuffer.allocate(1 + Long.BYTES * 4 + Integer.BYTES * 2)
                .order(ByteOrder.BIG_ENDIAN)
                .put((byte) 7).putLong(operation.stableCode()).putLong(ddlId).putLong(version)
                .putLong(secondaryObjectId).putInt(phase.stableCode()).putInt(chunk).array();
    }
}
