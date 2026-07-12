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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
