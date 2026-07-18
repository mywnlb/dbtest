package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
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
import org.junit.jupiter.api.function.Executable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** INDEX 页完整结构校验：合法生命周期通过，header/链/记录区间/directory group 损坏统一 fail-closed。 */
class RecordPageStructureValidatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));
    private static final long INDEX_ID = 7L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);

    /**
     * 验证 {@code acceptsEmptyAndAllSupportedPageMutationOutcomes} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void acceptsEmptyAndAllSupportedPageMutationOutcomes() {
        onPage((page, guard) -> {
            page.format(INDEX_ID, 0);
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));

            List<Integer> offsets = prepare(page, 12);
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));

            new RecordPageUpdater(registry).update(page, PAGE, offsets.get(1), row(2, "short"), key(), schema());
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));

            int purgeOffset = page.recordOffsetsInOrder().get(2);
            new RecordPageDeleter().deleteMark(page, purgeOffset);
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));
            new RecordPagePurger().purge(page, purgeOffset);
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));

            new RecordPageReorganizer().reorganize(page);
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));
        });
    }

    /**
     * 验证 {@code rejectsHeaderGeometryAndSystemRecordCorruption} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsHeaderGeometryAndSystemRecordCorruption() {
        onPage((page, guard) -> {
            page.format(INDEX_ID, 0);
            PageU16.put(guard, IndexPageHeaderLayout.N_DIR_SLOTS, 1);
            PageDirectoryCorruptedException headerError = assertCorrupted(
                    () -> RecordPageStructureValidator.validate(page));
            assertNotNull(headerError.getCause(), "lower-level header validation must be retained as cause");

            page.format(INDEX_ID, 0);
            PageU16.put(guard, IndexPageHeaderLayout.HEAP_TOP, page.dirStart() + 1);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            page.format(INDEX_ID, 0);
            PageU16.put(guard, IndexPageHeaderLayout.N_RECS, 1);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            page.format(INDEX_ID, 0);
            guard.writeBytes(IndexPageLayout.INFIMUM_OFFSET + IndexPageLayout.REC_FLAGS_FIELD_OFFSET,
                    new byte[]{0});
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            page.format(INDEX_ID, 0);
            guard.writeBytes(IndexPageLayout.INFIMUM_OFFSET + IndexPageLayout.REC_HEADER_BYTES,
                    new byte[]{'X'});
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            page.format(INDEX_ID, 0);
            page.setNextRecord(page.supremumOffset(), page.infimumOffset());
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));
        });
    }

    /**
     * 验证 {@code rejectsBrokenNextRecordChain} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsBrokenNextRecordChain() {
        onPage((page, guard) -> {
            List<Integer> offsets = prepare(page, 3);
            page.setNextRecord(page.infimumOffset(), 0);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 3);
            page.setNextRecord(offsets.get(2), offsets.get(0));
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            prepare(page, 3);
            page.setNextRecord(page.infimumOffset(), page.header().heapTop());
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));
        });
    }

    /**
     * 验证 {@code rejectsInvalidUserRecordHeaderCountAndPhysicalOverlap} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsInvalidUserRecordHeaderCountAndPhysicalOverlap() {
        onPage((page, guard) -> {
            List<Integer> offsets = prepare(page, 3);
            PageU16.put(guard, offsets.get(0) + 6, IndexPageLayout.REC_HEADER_BYTES - 1);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 3);
            PageU16.put(guard, offsets.get(0) + 6, offsets.get(1) - offsets.get(0) + 1);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 3);
            page.setHeapNo(offsets.get(1), page.recordHeaderAt(offsets.get(0)).heapNo());
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 3);
            guard.writeBytes(offsets.get(0) + IndexPageLayout.REC_FLAGS_FIELD_OFFSET,
                    new byte[]{(byte) (RecordType.INFIMUM.code() << 2)});
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            prepare(page, 3);
            PageU16.put(guard, IndexPageHeaderLayout.N_RECS, 2);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));
        });
    }

    /**
     * 验证 {@code rejectsSentinelSlotsOwnerOrderAndNOwnedMismatch} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsSentinelSlotsOwnerOrderAndNOwnedMismatch() {
        onPage((page, guard) -> {
            List<Integer> offsets = prepare(page, 24);
            page.directory().setSlot(0, offsets.get(0));
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 24);
            RecordPageDirectory directory = page.directory();
            directory.setSlot(directory.slotCount() - 1, offsets.get(offsets.size() - 1));
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            prepare(page, 24);
            directory = page.directory();
            directory.setSlot(2, directory.slot(1));
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            prepare(page, 24);
            directory = page.directory();
            int firstOwner = directory.slot(1);
            int secondOwner = directory.slot(2);
            directory.setSlot(1, secondOwner);
            directory.setSlot(2, firstOwner);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            prepare(page, 24);
            directory = page.directory();
            int owner = directory.slot(1);
            page.setNOwned(owner, page.recordHeaderAt(owner).nOwned() + 1);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 24);
            directory = page.directory();
            Set<Integer> owners = new HashSet<>();
            for (int slot = 0; slot < directory.slotCount(); slot++) {
                owners.add(directory.slot(slot));
            }
            int interior = offsets.stream().filter(offset -> !owners.contains(offset)).findFirst().orElseThrow();
            page.setNOwned(interior, 1);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));
        });
    }

    /** 多 fragment、first-fit 跳过、exact/oversized reuse、move/shrink/reorganize 都必须满足宽松但安全的账本边界。 */
    @Test
    void acceptsSupportedGarbageListAndUnlinkedFragmentStates() {
        onPage((page, guard) -> {
            page.format(INDEX_ID, 0);
            int big = inserter.insert(page, PAGE, row(1, "x".repeat(100)), key(), schema()).pageOffset();
            int small = inserter.insert(page, PAGE, row(2, "s"), key(), schema()).pageOffset();
            inserter.insert(page, PAGE, row(3, "live"), key(), schema());
            purge(page, big);
            purge(page, small); // FREE: small -> big，先遇到不够大的 small。
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));

            inserter.insert(page, PAGE, row(4, "y".repeat(50)), key(), schema()); // 跳过 small，oversized reuse big。
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));
            inserter.insert(page, PAGE, row(5, "z"), key(), schema()); // exact-size reuse small。
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));

            int live = page.recordOffsetsInOrder().get(0);
            long liveId = ((ColumnValue.IntValue) new RecordCursor(page, live, schema(), registry)
                    .readColumn(new ColumnId(0))).value();
            new RecordPageUpdater(registry).update(
                    page, PAGE, live, row(liveId, "m".repeat(150)), key(), schema());
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));
            new RecordPageReorganizer().reorganize(page);
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));

            page.format(INDEX_ID, 0);
            int shrink = inserter.insert(page, PAGE, row(10, "q".repeat(100)), key(), schema()).pageOffset();
            new RecordPageUpdater(registry).update(page, PAGE, shrink, row(10, "q"), key(), schema());
            assertEquals(0, page.header().free());
            assertTrue(page.header().garbage() > 0, "in-place shrink has counted but unlinked garbage");
            assertDoesNotThrow(() -> RecordPageStructureValidator.validate(page));
        });
    }

    /**
     * 验证 {@code rejectsGarbageFreeChainGeometryTypeAndCycleCorruption} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsGarbageFreeChainGeometryTypeAndCycleCorruption() {
        onPage((page, guard) -> {
            List<Integer> offsets = prepare(page, 3);
            purge(page, offsets.get(0));
            page.writeHeader(page.header().withFree(page.header().heapTop()));
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 3);
            purge(page, offsets.get(0));
            page.setNextRecord(offsets.get(0), offsets.get(0));
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 3);
            purge(page, offsets.get(0));
            PageU16.put(guard, offsets.get(0) + 6, IndexPageLayout.REC_HEADER_BYTES - 1);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 3);
            purge(page, offsets.get(0));
            guard.writeBytes(offsets.get(0) + IndexPageLayout.REC_FLAGS_FIELD_OFFSET,
                    new byte[]{(byte) (RecordType.SUPREMUM.code() << 2)});
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));
        });
    }

    /**
     * 验证 {@code rejectsGarbageHeapIdentityAndPhysicalRangeConflicts} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsGarbageHeapIdentityAndPhysicalRangeConflicts() {
        onPage((page, guard) -> {
            List<Integer> offsets = prepare(page, 3);
            purge(page, offsets.get(0));
            page.setHeapNo(offsets.get(0), page.recordHeaderAt(offsets.get(1)).heapNo());
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 4);
            purge(page, offsets.get(0));
            purge(page, offsets.get(2));
            page.setHeapNo(offsets.get(0), page.recordHeaderAt(offsets.get(2)).heapNo());
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 3);
            purge(page, offsets.get(0));
            PageU16.put(guard, offsets.get(0) + 6, offsets.get(1) - offsets.get(0) + 1);
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));
        });
    }

    /**
     * 验证 {@code rejectsGarbageBytesOutsideLinkedAndPhysicalBounds} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsGarbageBytesOutsideLinkedAndPhysicalBounds() {
        onPage((page, guard) -> {
            List<Integer> offsets = prepare(page, 2);
            purge(page, offsets.get(0));
            int linkedCapacity = page.recordHeaderAt(offsets.get(0)).recordLength();
            page.writeHeader(page.header().withGarbage(linkedCapacity - 1));
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));

            offsets = prepare(page, 1);
            int liveBytes = page.recordHeaderAt(offsets.get(0)).recordLength();
            int heapSpan = page.header().heapTop() - IndexPageLayout.USER_RECORDS_START;
            page.writeHeader(page.header().withGarbage(heapSpan - liveBytes + 1));
            assertCorrupted(() -> RecordPageStructureValidator.validate(page));
        });
    }

    private static void purge(RecordPage page, int offset) {
        new RecordPageDeleter().deleteMark(page, offset);
        new RecordPagePurger().purge(page, offset);
    }

    private List<Integer> prepare(RecordPage page, int count) {
        page.format(INDEX_ID, 0);
        List<Integer> offsets = new ArrayList<>();
        for (int id = 1; id <= count; id++) {
            offsets.add(inserter.insert(page, PAGE, row(id, "value-" + id), key(), schema()).pageOffset());
        }
        return offsets;
    }

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(200, true), 1)));
    }

    private static IndexKeyDef key() {
        return new IndexKeyDef(INDEX_ID,
                List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static LogicalRecord row(long id, String payload) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(payload)), false, RecordType.CONVENTIONAL);
    }

    private static PageDirectoryCorruptedException assertCorrupted(Executable executable) {
        return assertThrows(PageDirectoryCorruptedException.class, executable);
    }

    private interface PageBody {
        void run(RecordPage page, PageGuard guard);
    }

    private void onPage(PageBody body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("validator.ibd"), PS, PageNo.of(4));
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 4);
             PageGuard guard = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
            body.run(new RecordPage(guard, PS), guard);
        }
    }
}
