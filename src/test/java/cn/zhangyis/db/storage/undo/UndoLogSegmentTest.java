package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.exception.NoFreeSpaceException;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoLogBatch;
import cn.zhangyis.db.storage.redo.RedoRecord;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaRecord;
import cn.zhangyis.db.storage.redo.UndoRecordPayloadRecord;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3b UndoLogSegment 单页：create→append→RollPointer→readRecord、log header 计数、
 * forEach 有序遍历、NULL 指针拒绝。
 */
class UndoLogSegmentTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(64, true), 1)), true);
    }

    private static IndexKeyDef keyDef() {
        return new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private UndoRecord rec(long undoNo, long id, RollPointer prev) {
        return UndoRecord.insert(UndoNo.of(undoNo), TransactionId.of(7),
                1L, 9L, List.of(new ColumnValue.IntValue(id)), prev);
    }

    private static boolean rangesOverlap(int leftOffset, int leftLength, int rightOffset, int rightLength) {
        return leftOffset < rightOffset + rightLength && rightOffset < leftOffset + leftLength;
    }

    /**
     * 验证 {@code createAppendReadBackSinglePage} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void createAppendReadBackSinglePage() {
        onSegment(seg -> {
            UndoRecord r = rec(1, 100, RollPointer.NULL);
            RollPointer rp = seg.append(r, keyDef(), schema());
            assertFalse(rp.isNull());
            assertTrue(rp.insert());
            assertEquals(seg.firstPageId().pageNo(), rp.pageNo());
            assertEquals(r, seg.readRecord(rp, keyDef(), schema()));
            assertEquals(1L, seg.logRecordCount());
            assertEquals(1L, seg.logLastUndoNo().value());
            assertEquals(new UndoLogicalHead(UndoNo.of(1), rp), seg.logicalHead(),
                    "append must publish the record as the persistent logical chain head");
        });
    }

    /** 部分回滚只移动逻辑头，物理 record 计数和 append 高水位必须保留。 */
    @Test
    void updateLogicalHeadKeepsPhysicalAppendHighWater() {
        onSegment(seg -> {
            RollPointer rp1 = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            RollPointer rp2 = seg.append(rec(2, 101, rp1), keyDef(), schema());
            assertEquals(new UndoLogicalHead(UndoNo.of(2), rp2), seg.logicalHead());

            seg.updateLogicalHead(
                    new UndoLogicalHead(UndoNo.of(2), rp2),
                    new UndoLogicalHead(UndoNo.of(1), rp1),
                    keyDef(), schema());

            assertEquals(new UndoLogicalHead(UndoNo.of(1), rp1), seg.logicalHead());
            assertEquals(2L, seg.logRecordCount());
            assertEquals(UndoNo.of(2), seg.logLastUndoNo());
            assertEquals(rec(2, 101, rp1), seg.readRecord(rp2, keyDef(), schema()),
                    "rolled-back branch remains physically readable until segment purge");
        });
    }

    /** append 的事务、undoNo 和 predecessor 必须在任何槽写入前校验，失败不能留下无 redo 的 orphan bytes。 */
    @Test
    void appendRejectsBrokenLogicalChainBeforeMutation() {
        onSegment(seg -> {
            RollPointer rp1 = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            UndoLogicalHead originalHead = new UndoLogicalHead(UndoNo.of(1), rp1);

            assertThrows(DatabaseValidationException.class,
                    () -> seg.append(rec(2, 101, RollPointer.NULL), keyDef(), schema()),
                    "predecessor must equal the persistent logical head");
            RollPointer rp3 = seg.append(rec(3, 101, rp1), keyDef(), schema());
            UndoRecord foreignTxn = UndoRecord.insert(UndoNo.of(4), TransactionId.of(8),
                    1L, 9L, List.of(new ColumnValue.IntValue(101)), rp3);
            assertThrows(DatabaseValidationException.class,
                    () -> seg.append(foreignTxn, keyDef(), schema()));

            assertEquals(2L, seg.logRecordCount());
            assertEquals(UndoNo.of(3), seg.logLastUndoNo(),
                    "independent logs may have local undoNo gaps");
            assertEquals(new UndoLogicalHead(UndoNo.of(3), rp3), seg.logicalHead());
            assertEquals(rec(1, 100, RollPointer.NULL), seg.readRecord(rp1, keyDef(), schema()));
        });
    }

    /** 同一事务 undo 逻辑链允许跨表/索引；append 只能 schema-free 校验 predecessor identity。 */
    @Test
    void appendAcceptsPredecessorFromAnotherTableAndIndex() {
        onSegment(seg -> {
            RollPointer first = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            IndexKeyDef secondKey = new IndexKeyDef(10L,
                    List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
            TableSchema secondSchema = new TableSchema(2, List.of(
                    new ColumnDef(new ColumnId(0), "other_id", ColumnType.intType(false, false), 0)), true);
            UndoRecord second = UndoRecord.insert(UndoNo.of(2), TransactionId.of(7),
                    2L, 10L, List.of(new ColumnValue.IntValue(200)), first);

            RollPointer secondPointer = seg.append(second, secondKey, secondSchema);

            assertEquals(second, seg.readRecord(secondPointer, secondKey, secondSchema));
            assertEquals(rec(1, 100, RollPointer.NULL), seg.readRecord(first, keyDef(), schema()));
            assertEquals(new UndoLogicalHead(UndoNo.of(2), secondPointer), seg.logicalHead());
        });
    }

    /** logical pair 必须由一条 15B metadata delta 覆盖，避免 redo 中出现可拆分的半边界。 */
    @Test
    void appendWritesLogicalHeadAsSingleMetadataDelta() {
        onAccess((mgr, access) -> {
            MiniTransaction create = mgr.begin();
            UndoLogSegment created = access.create(create, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            PageId firstPageId = created.firstPageId();
            mgr.commit(create);
            int baseline = mgr.redoLogManager().bufferedRecords().size();
            int baselineBatches = mgr.redoLogManager().bufferedBatches().size();

            MiniTransaction append = mgr.begin();
            UndoLogSegment writable = access.open(append, firstPageId,
                    cn.zhangyis.db.storage.buf.PageLatchMode.EXCLUSIVE);
            RollPointer pointer = writable.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            mgr.commit(append);

            List<RedoRecord> allRecords = mgr.redoLogManager().bufferedRecords();
            List<RedoRecord> appended = allRecords.subList(baseline, allRecords.size());
            List<RedoLogBatch> batches = mgr.redoLogManager().bufferedBatches();
            assertEquals(baselineBatches + 1, batches.size());
            assertEquals(appended, batches.getLast().records(),
                    "payload, physical high-water and logical head must share one MTR redo batch");
            List<UndoMetadataDeltaRecord> headDeltas = appended.stream()
                    .filter(UndoMetadataDeltaRecord.class::isInstance)
                    .map(UndoMetadataDeltaRecord.class::cast)
                    .filter(delta -> delta.pageId().equals(firstPageId)
                            && delta.kind() == UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD
                            && delta.offset() == UndoPageLayout.LOGICAL_LAST_UNDO_NO)
                    .toList();
            byte[] expected = ByteBuffer.allocate(Long.BYTES + RollPointer.BYTES)
                    .putLong(1L).put(pointer.encode()).array();
            assertEquals(1, headDeltas.size());
            assertTrue(Arrays.equals(expected, headDeltas.getFirst().afterImage()));
            assertTrue(appended.stream().anyMatch(record -> record instanceof UndoRecordPayloadRecord payload
                            && payload.pageId().equals(PageId.of(UNDO_SPACE, pointer.pageNo()))
                            && payload.undoNo().equals(UndoNo.of(1))),
                    "the same batch must contain the appended undo record payload");
            assertTrue(appended.stream().anyMatch(record -> record instanceof UndoMetadataDeltaRecord delta
                            && delta.pageId().equals(firstPageId)
                            && delta.offset() == UndoPageLayout.LOG_LAST_UNDO_NO
                            && ByteBuffer.wrap(delta.afterImage()).getLong() == 1L),
                    "the same batch must contain the physical append high-water");
            assertFalse(appended.stream().anyMatch(record -> record instanceof PageBytesRecord bytes
                            && bytes.pageId().equals(firstPageId)
                            && rangesOverlap(bytes.offset(), bytes.bytes().length,
                                    UndoPageLayout.LOGICAL_LAST_UNDO_NO, expected.length)),
                    "logical head metadata delta must replace its overlapping physical PAGE_BYTES");
        });
    }

    /**
     * 验证 {@code logHeaderCountsAdvancePerAppend} 对应的Undo 日志行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void logHeaderCountsAdvancePerAppend() {
        onSegment(seg -> {
            RollPointer rp1 = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            RollPointer rp2 = seg.append(rec(2, 101, rp1), keyDef(), schema());
            seg.append(rec(3, 102, rp2), keyDef(), schema());
            assertEquals(3L, seg.logRecordCount());
            assertEquals(3L, seg.logLastUndoNo().value());
        });
    }

    /**
     * 验证 {@code forEachRecordReturnsAllInOrderSinglePage} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void forEachRecordReturnsAllInOrderSinglePage() {
        onSegment(seg -> {
            RollPointer rp1 = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            seg.append(rec(2, 101, rp1), keyDef(), schema());
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, keyDef(), schema());
            assertEquals(List.of(rec(1, 100, RollPointer.NULL), rec(2, 101, rp1)), got);
        });
    }

    /**
     * 验证 {@code readRecordRejectsNullPointer} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void readRecordRejectsNullPointer() {
        onSegment(seg ->
                assertThrows(UndoLogFormatException.class,
                        () -> seg.readRecord(RollPointer.NULL, keyDef(), schema())));
    }

    /**
     * 验证 {@code markCommittedWritesStateAndCommitNoForRecoveryHistory} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void markCommittedWritesStateAndCommitNoForRecoveryHistory() {
        onSegment(seg -> {
            assertTrue(seg.isActive());
            assertFalse(seg.isCommitted());

            seg.markCommitted(TransactionNo.of(42));

            assertFalse(seg.isActive());
            assertTrue(seg.isCommitted());
            assertEquals(TransactionNo.of(42), seg.committedTransactionNo(),
                    "COMMIT_NO is the recovery-time history ordering key");
        });
    }

    /** phase one 只写 PREPARED 状态，提交号必须保持 NONE，供上层 phase-two 决议后再分配。 */
    @Test
    void markPreparedWritesStableStateWithoutCommitNumber() {
        onSegment(segment -> {
            assertTrue(segment.isActive());

            segment.markPrepared();

            assertFalse(segment.isActive());
            assertTrue(segment.isPrepared());
            assertEquals(TransactionNo.NONE, segment.committedTransactionNo());
        });
    }

    /** PREPARED 使用现有 state u8 的追加 code，并由 undo metadata delta redo 精确覆盖。 */
    @Test
    void markPreparedAppendsUndoMetadataDeltaRedo() {
        onAccess((manager, access) -> {
            MiniTransaction mtr = manager.begin();
            UndoLogSegment segment = access.create(
                    mtr, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            segment.markPrepared();
            manager.commit(mtr);

            assertTrue(manager.redoLogManager().bufferedRecords().stream()
                            .anyMatch(record -> record instanceof UndoMetadataDeltaRecord delta
                                    && delta.pageId().equals(segment.firstPageId())
                                    && delta.kind() == UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD
                                    && delta.offset() == UndoPageLayout.STATE
                                    && Arrays.equals(new byte[]{UndoPageLayout.STATE_PREPARED},
                                    delta.afterImage())),
                    "STATE=PREPARED must use the stable undo metadata redo path");
        });
    }

    /**
     * 验证 {@code markCommittedAppendsUndoMetadataDeltaRedoForStateAndCommitNo} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void markCommittedAppendsUndoMetadataDeltaRedoForStateAndCommitNo() {
        onAccess((mgr, access) -> {
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            seg.markCommitted(TransactionNo.of(42));
            mgr.commit(m);

            List<RedoRecord> records = mgr.redoLogManager().bufferedRecords();
            PageId firstPage = seg.firstPageId();
            assertTrue(records.stream().anyMatch(record -> record instanceof UndoMetadataDeltaRecord delta
                            && delta.pageId().equals(firstPage)
                            && delta.kind() == UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD
                            && delta.offset() == UndoPageLayout.STATE
                            && Arrays.equals(new byte[]{UndoPageLayout.STATE_COMMITTED}, delta.afterImage())),
                    "STATE=COMMITTED must have logical undo metadata redo");
            assertTrue(records.stream().anyMatch(record -> record instanceof UndoMetadataDeltaRecord delta
                            && delta.pageId().equals(firstPage)
                            && delta.kind() == UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD
                            && delta.offset() == UndoPageLayout.COMMIT_NO
                            && Arrays.equals(longBytes(42L), delta.afterImage())),
                    "COMMIT_NO after-image must have logical undo metadata redo");
        });
    }

    /**
     * 验证 {@code appendRecordPayloadUsesLogicalRedoWithoutPhysicalSlotBytes} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void appendRecordPayloadUsesLogicalRedoWithoutPhysicalSlotBytes() {
        onAccess((mgr, access) -> {
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            RollPointer rp = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            PageId firstPage = seg.firstPageId();
            mgr.commit(m);

            List<RedoRecord> records = mgr.redoLogManager().bufferedRecords();
            assertTrue(records.stream().anyMatch(record -> record instanceof UndoRecordPayloadRecord payload
                            && payload.pageId().equals(firstPage)
                            && payload.transactionId().equals(TransactionId.of(7))
                            && payload.undoNo().equals(UndoNo.of(1))
                            && payload.recordOffset() == rp.offset()),
                    "ordinary undo record slot must have dedicated logical redo");
            assertFalse(records.stream().anyMatch(record -> record instanceof PageBytesRecord bytes
                            && bytes.pageId().equals(firstPage)
                            && bytes.offset() == rp.offset()),
                    "covered undo record slot PAGE_BYTES should be filtered at commit");
        });
    }

    private static TableSchema bigSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "k", ColumnType.varchar(20000, false), 0)), true);
    }

    private static IndexKeyDef bigKeyDef() {
        return new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private UndoRecord bigRec(long undoNo, String key, RollPointer prev) {
        return UndoRecord.insert(UndoNo.of(undoNo), TransactionId.of(7),
                1L, 9L, List.of(new ColumnValue.StringValue(key)), prev);
    }

    private static String bigKey(long undoNo) {
        return String.format("%05d", undoNo) + "x".repeat(4995);
    }

    /**
     * 验证 {@code growthAllocatesLinksNewPageAndReadsAcross} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void growthAllocatesLinksNewPageAndReadsAcross() {
        onSegment(seg -> {
            RollPointer rp1 = seg.append(bigRec(1, bigKey(1), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp2 = seg.append(bigRec(2, bigKey(2), rp1), bigKeyDef(), bigSchema());
            RollPointer rp3 = seg.append(bigRec(3, bigKey(3), rp2), bigKeyDef(), bigSchema());
            RollPointer rp4 = seg.append(bigRec(4, bigKey(4), rp3), bigKeyDef(), bigSchema());
            assertEquals(seg.firstPageId().pageNo(), rp1.pageNo());
            assertEquals(seg.firstPageId().pageNo(), rp3.pageNo());
            assertNotEquals(seg.firstPageId().pageNo(), rp4.pageNo());
            assertEquals(seg.lastPageId().pageNo(), rp4.pageNo());
            assertEquals(4L, seg.logRecordCount());
            assertEquals(4L, seg.logLastUndoNo().value());
            assertEquals(bigRec(4, bigKey(4), rp3), seg.readRecord(rp4, bigKeyDef(), bigSchema()));
            assertEquals(bigRec(1, bigKey(1), RollPointer.NULL), seg.readRecord(rp1, bigKeyDef(), bigSchema()));
        });
    }

    /**
     * 0.14b：undo 页链 grow 是真实多页消费者。第四条大 undo record 放不进 first 页，必须在分配新 undo 页前
     * 用 UNDO reservation 预扩容量；未接消费者时 grow 会直接从当前 extent 取空闲页，物理大小停在 128。
     */
    @Test
    void growReservesUndoSpaceBeforeAllocatingChainPage() {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo-reserve.ibu"), PageNo.of(64));
            mgr.commit(boot);

            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            RollPointer rp1 = seg.append(bigRec(1, bigKey(1), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp2 = seg.append(bigRec(2, bigKey(2), rp1), bigKeyDef(), bigSchema());
            RollPointer rp3 = seg.append(bigRec(3, bigKey(3), rp2), bigKeyDef(), bigSchema());
            assertEquals(PageNo.of(128), store.currentSizeInPages(UNDO_SPACE),
                    "first undo segment page allocation autoextends once");

            RollPointer rp4 = seg.append(bigRec(4, bigKey(4), rp3), bigKeyDef(), bigSchema());

            assertEquals(PageNo.of(192), store.currentSizeInPages(UNDO_SPACE),
                    "0.14b UNDO reservation preextends before grow allocatePage");
            assertEquals(seg.firstPageId().pageNo(), rp1.pageNo());
            assertEquals(seg.firstPageId().pageNo(), rp3.pageNo());
            assertEquals(seg.lastPageId().pageNo(), rp4.pageNo());
            assertEquals(4L, seg.logRecordCount());
            mgr.commit(m);

            MiniTransaction read = mgr.begin();
            assertEquals(PageNo.of(192), disk.usage(read, UNDO_SPACE).currentSizeInPages());
            mgr.commit(read);
        }
    }

    /**
     * 0.14b：当 undo grow 预留无法扩到下一 extent 时，必须在分配、格式化、FIL 链接和 first 页 header 推进前失败。
     * 这里把文件上限固定在 128 页；前三条大记录仍在 first 页，第四条本会 grow。正确实现应抛 `NoFreeSpaceException`，
     * 且 lastPage/log header/遍历结果保持在前三条记录。
     */
    @Test
    void growReservationFailureDoesNotLinkNewUndoPage() {
        PageStore store = new LimitedPageStore(128);
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo-grow-limit.ibu"), PageNo.of(64));
            mgr.commit(boot);

            MiniTransaction m = mgr.begin();
            boolean committed = false;
            try {
                UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
                List<UndoRecord> expected = new ArrayList<>();
                RollPointer previous = RollPointer.NULL;
                for (long i = 1; i <= 3; i++) {
                    UndoRecord rec = bigRec(i, bigKey(i), previous);
                    expected.add(rec);
                    previous = seg.append(rec, bigKeyDef(), bigSchema());
                }
                assertEquals(PageNo.of(128), store.currentSizeInPages(UNDO_SPACE));

                RollPointer head = previous;
                assertThrows(NoFreeSpaceException.class,
                        () -> seg.append(bigRec(4, bigKey(4), head), bigKeyDef(), bigSchema()));

                assertEquals(seg.firstPageId(), seg.lastPageId(),
                        "failed reservation must not allocate or link a chain page");
                assertEquals(3L, seg.logRecordCount());
                assertEquals(3L, seg.logLastUndoNo().value());
                List<UndoRecord> got = new ArrayList<>();
                seg.forEachRecord(got::add, bigKeyDef(), bigSchema());
                assertEquals(expected, got);
                assertEquals(PageNo.of(128), store.currentSizeInPages(UNDO_SPACE));
                mgr.commit(m);
                committed = true;
            } finally {
                if (!committed) {
                    mgr.rollbackUncommitted(m);
                }
            }
        }
    }

    /**
     * 验证 {@code forEachTraversesAllPagesInOrder} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void forEachTraversesAllPagesInOrder() {
        onSegment(seg -> {
            List<UndoRecord> expected = new ArrayList<>();
            RollPointer previous = RollPointer.NULL;
            for (long i = 1; i <= 5; i++) {
                UndoRecord r = bigRec(i, bigKey(i), previous);
                expected.add(r);
                previous = seg.append(r, bigKeyDef(), bigSchema());
            }
            assertNotEquals(seg.firstPageId().pageNo(), seg.lastPageId().pageNo());
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, bigKeyDef(), bigSchema());
            assertEquals(expected, got);
        });
    }

    /**
     * 验证 {@code prevRollPointerChainsAcrossPages} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void prevRollPointerChainsAcrossPages() {
        onSegment(seg -> {
            RollPointer rp1 = seg.append(
                    bigRec(1, bigKey(1), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp2 = seg.append(bigRec(2, bigKey(2), rp1), bigKeyDef(), bigSchema());
            RollPointer rp3 = seg.append(bigRec(3, bigKey(3), rp2), bigKeyDef(), bigSchema());
            RollPointer rp4 = seg.append(bigRec(4, bigKey(4), rp3), bigKeyDef(), bigSchema());
            assertNotEquals(rp3.pageNo(), rp4.pageNo());
            UndoRecord back4 = seg.readRecord(rp4, bigKeyDef(), bigSchema());
            assertEquals(rp3, back4.prevRollPointer());
            assertEquals(bigRec(3, bigKey(3), rp2),
                    seg.readRecord(back4.prevRollPointer(), bigKeyDef(), bigSchema()));
        });
    }

    /**
     * 验证 {@code oversizedRecordUsesExternalPayloadWithoutGrowingMainUndoChain} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void oversizedRecordUsesExternalPayloadWithoutGrowingMainUndoChain() {
        onSegment(seg -> {
            String huge = "y".repeat(16300);
            UndoRecord big = UndoRecord.insert(UndoNo.of(1), TransactionId.of(7),
                    1L, 9L, List.of(new ColumnValue.StringValue(huge)), RollPointer.NULL);
            RollPointer pointer = seg.append(big, bigKeyDef(), bigSchema());
            assertEquals(seg.firstPageId(), seg.lastPageId());
            assertEquals(1L, seg.logRecordCount());
            assertEquals(1L, seg.logLastUndoNo().value());
            assertEquals(big, seg.readRecord(pointer, bigKeyDef(), bigSchema()));
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, bigKeyDef(), bigSchema());
            assertEquals(List.of(big), got);
        });
    }

    /**
     * 验证 {@code readRecordRejectsPointerFromOtherSegment} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void readRecordRejectsPointerFromOtherSegment() {
        onAccess((mgr, access) -> {
            MiniTransaction m1 = mgr.begin();
            UndoLogSegment segB = access.create(m1, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            RollPointer rpB = segB.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            mgr.commit(m1);

            MiniTransaction m2 = mgr.begin();
            UndoLogSegment segA = access.create(m2, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            assertThrows(UndoLogFormatException.class, () -> segA.readRecord(rpB, keyDef(), schema()));
            mgr.commit(m2);
        });
    }

    // ---- T1.4：readRecordByRollPointer（MVCC 跨段直读 + 损坏拒绝） ----

    private UndoRecord updateRec(long undoNo, long id, RollPointer prev) {
        return UndoRecord.update(UndoNo.of(undoNo), TransactionId.of(7), 1L, 9L,
                java.util.List.of(new ColumnValue.IntValue(id)),
                java.util.List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue("old-" + id)),
                new cn.zhangyis.db.storage.record.format.HiddenColumns(TransactionId.of(3), prev), prev);
    }

    /**
     * 验证 {@code readRecordByRollPointerReadsInsertAndUpdate} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void readRecordByRollPointerReadsInsertAndUpdate() {
        onAccess((mgr, access) -> {
            MiniTransaction insertMtr = mgr.begin();
            UndoLogSegment insertLog = access.create(
                    insertMtr, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            RollPointer insRp = insertLog.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            mgr.commit(insertMtr);
            MiniTransaction updateMtr = mgr.begin();
            UndoLogSegment updateLog = access.create(
                    updateMtr, UNDO_SPACE, TransactionId.of(7), UndoLogKind.UPDATE);
            UndoRecord update = UndoRecord.update(UndoNo.of(2), TransactionId.of(7), 1L, 9L,
                    List.of(new ColumnValue.IntValue(100)),
                    List.of(new ColumnValue.IntValue(100), new ColumnValue.StringValue("old-100")),
                    new cn.zhangyis.db.storage.record.format.HiddenColumns(TransactionId.of(3), insRp),
                    RollPointer.NULL);
            RollPointer updRp = updateLog.append(update, keyDef(), schema());
            mgr.commit(updateMtr);

            MiniTransaction r = mgr.begin();
            UndoRecord ins = access.readRecordByRollPointer(r, UNDO_SPACE, insRp, keyDef(), schema());
            UndoRecord upd = access.readRecordByRollPointer(r, UNDO_SPACE, updRp, keyDef(), schema());
            mgr.rollbackUncommitted(r);

            assertEquals(UndoRecordType.INSERT_ROW, ins.type());
            assertEquals(UndoRecordType.UPDATE_ROW, upd.type());
            assertEquals(java.util.List.of(new ColumnValue.IntValue(100),
                    new ColumnValue.StringValue("old-100")), upd.oldColumnValues());
        });
    }

    /**
     * 验证 {@code readRecordByRollPointerRejectsCorruption} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void readRecordByRollPointerRejectsCorruption() {
        onAccess((mgr, access) -> {
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            RollPointer insRp = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            mgr.commit(m);

            MiniTransaction r = mgr.begin();
            // NULL 指针
            assertThrows(UndoLogFormatException.class,
                    () -> access.readRecordByRollPointer(r, UNDO_SPACE, RollPointer.NULL, keyDef(), schema()));
            // insert 位与记录类型不符（INSERT 记录但指针 insert=false）
            RollPointer wrongBit = new RollPointer(false, insRp.pageNo(), insRp.offset());
            assertThrows(UndoLogFormatException.class,
                    () -> access.readRecordByRollPointer(r, UNDO_SPACE, wrongBit, keyDef(), schema()));
            // indexId 不符
            IndexKeyDef otherIndex = new IndexKeyDef(99L, java.util.List.of(
                    new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
            assertThrows(UndoLogFormatException.class,
                    () -> access.readRecordByRollPointer(r, UNDO_SPACE, insRp, otherIndex, schema()));
            // 越界 offset（超出 record area）
            RollPointer badOffset = new RollPointer(true, insRp.pageNo(), 16000);
            assertThrows(UndoLogFormatException.class,
                    () -> access.readRecordByRollPointer(r, UNDO_SPACE, badOffset, keyDef(), schema()));
            mgr.rollbackUncommitted(r);
        });
    }

    /**
     * 验证 {@code appendStampsRollPointerInsertFlagByRecordType} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void appendStampsRollPointerInsertFlagByRecordType() {
        onAccess((mgr, access) -> {
            MiniTransaction insertMtr = mgr.begin();
            UndoLogSegment insertLog = access.create(
                    insertMtr, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            RollPointer insRp = insertLog.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            assertTrue(insRp.insert(), "INSERT_ROW append must stamp insert=true");
            mgr.commit(insertMtr);

            MiniTransaction updateMtr = mgr.begin();
            UndoLogSegment updateLog = access.create(
                    updateMtr, UNDO_SPACE, TransactionId.of(7), UndoLogKind.UPDATE);
            UndoRecord upd = UndoRecord.update(UndoNo.of(2), TransactionId.of(7), 1L, 9L,
                    List.of(new ColumnValue.IntValue(100)),
                    List.of(new ColumnValue.IntValue(100), new ColumnValue.StringValue("old")),
                    new cn.zhangyis.db.storage.record.format.HiddenColumns(
                            TransactionId.of(3), new RollPointer(false, PageNo.of(65), 1)),
                    RollPointer.NULL);
            RollPointer updRp = updateLog.append(upd, keyDef(), schema());
            assertFalse(updRp.insert(), "UPDATE_ROW append must stamp insert=false");
            mgr.commit(updateMtr);
        });
    }

    /** v2 segment kind 是持久格式边界，不能靠 RollPointer.insert 位事后猜测或容忍混写。 */
    @Test
    void appendRejectsRecordTypeThatDoesNotBelongToSegmentKind() {
        onAccess((mgr, access) -> {
            UndoRecord update = updateRec(1, 100, RollPointer.NULL);
            MiniTransaction insertMtr = mgr.begin();
            UndoLogSegment insertLog = access.create(
                    insertMtr, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            assertThrows(UndoLogFormatException.class,
                    () -> insertLog.append(update, keyDef(), schema()));
            mgr.rollbackUncommitted(insertMtr);

            MiniTransaction updateMtr = mgr.begin();
            UndoLogSegment updateLog = access.create(
                    updateMtr, UNDO_SPACE, TransactionId.of(7), UndoLogKind.UPDATE);
            assertThrows(UndoLogFormatException.class,
                    () -> updateLog.append(rec(1, 100, RollPointer.NULL), keyDef(), schema()));
            mgr.rollbackUncommitted(updateMtr);
        });
    }

    /**
     * 验证 {@code forEachRecordWithPointerYieldsAddressesThatRoundTrip} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void forEachRecordWithPointerYieldsAddressesThatRoundTrip() {
        onSegment(seg -> {
            RollPointer rp1 = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            RollPointer rp2 = seg.append(rec(2, 101, rp1), keyDef(), schema());
            List<UndoRecord> recs = new ArrayList<>();
            List<RollPointer> pointers = new ArrayList<>();
            seg.forEachRecordWithPointer((r, rp) -> {
                recs.add(r);
                pointers.add(rp);
            }, keyDef(), schema());
            // 遍历给出的地址必须等于 append 返回的 RollPointer（purge 据此与聚簇 DB_ROLL_PTR 比对）
            assertEquals(List.of(rp1, rp2), pointers);
            // 每个地址 readRecord 回原记录，证明 pageNo+offset 正确
            assertEquals(recs.get(0), seg.readRecord(pointers.get(0), keyDef(), schema()));
            assertEquals(recs.get(1), seg.readRecord(pointers.get(1), keyDef(), schema()));
        });
    }

    private interface SegmentBody {
        void run(UndoLogSegment seg);
    }

    private interface AccessBody {
        void run(MiniTransactionManager mgr, UndoLogSegmentAccess access);
    }

    private void onSegment(SegmentBody body) {
        onAccess((mgr, access) -> {
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7), UndoLogKind.INSERT);
            body.run(seg);
            mgr.commit(m);
        });
    }

    private void onAccess(AccessBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, access);
        }
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    /**
     * 限制 ensureCapacity 上限的测试 PageStore，用来稳定模拟 UNDO reservation 阶段空间不足。
     */
    private static final class LimitedPageStore implements PageStore {

        private final PageStore delegate = new FileChannelPageStore();
        private final long maxPages;

        private LimitedPageStore(long maxPages) {
            this.maxPages = maxPages;
        }

        @Override
        public void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
            delegate.create(spaceId, path, pageSize, initialSizeInPages);
        }

        @Override
        public void open(SpaceId spaceId, Path path, PageSize pageSize) {
            delegate.open(spaceId, path, pageSize);
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            delegate.readPage(pageId, dst);
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
            delegate.writePage(pageId, src);
        }

        @Override
        public PageNo extend(SpaceId spaceId) {
            return delegate.extend(spaceId);
        }

        @Override
        public PageNo currentSizeInPages(SpaceId spaceId) {
            return delegate.currentSizeInPages(spaceId);
        }

        @Override
        public Path pathOf(SpaceId spaceId) {
            return delegate.pathOf(spaceId);
        }

        @Override
        public void force(SpaceId spaceId) {
            delegate.force(spaceId);
        }

        @Override
        public void forceAll() {
            delegate.forceAll();
        }

        @Override
        public void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages) {
            if (minSizeInPages.value() > maxPages) {
                throw new NoFreeSpaceException("test store cannot grow to " + minSizeInPages.value());
            }
            delegate.ensureCapacity(spaceId, minSizeInPages);
        }

        @Override
        public void truncate(SpaceId spaceId, PageNo targetSizeInPages) {
            delegate.truncate(spaceId, targetSizeInPages);
        }

        @Override
        public void close(SpaceId spaceId) {
            delegate.close(spaceId);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
