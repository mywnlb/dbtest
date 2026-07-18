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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RecordPageReorganizer：回收 garbage、保留 delete-marked、heapNo 稠密、链/目录不变量、key 仍可查。 */
class RecordPageReorganizerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);
    private final RecordPageSearch search = new RecordPageSearch(registry);
    private final RecordPageDeleter deleter = new RecordPageDeleter();
    private final RecordPagePurger purger = new RecordPagePurger();
    private final RecordPageReorganizer reorganizer = new RecordPageReorganizer();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey k(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
    }

    private interface PageBody { void run(RecordPage rp, TableSchema schema); }

    private void onPage(PageBody body) {
        TableSchema schema = schema();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);
                body.run(rp, schema);
            }
        }
    }

    private List<Long> idsInOrder(RecordPage rp, TableSchema schema) {
        List<Long> ids = new ArrayList<>();
        for (int off : rp.recordOffsetsInOrder()) {
            ids.add(((ColumnValue.IntValue) new RecordCursor(rp, off, schema, registry)
                    .readColumn(new ColumnId(0))).value());
        }
        return ids;
    }

    /**
     * 验证 {@code reorganizeReclaimsGarbageAndKeepsOrder} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void reorganizeReclaimsGarbageAndKeepsOrder() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 12; i++) {
                inserter.insert(rp, PAGE, row(i, "name-" + i), kd, schema);
            }
            // purge 3 条产生 garbage。
            for (int i : new int[]{3, 6, 9}) {
                int off = search.findEqual(rp, k(i), kd, schema).getAsInt();
                deleter.deleteMark(rp, off);
                purger.purge(rp, off);
            }
            assertTrue(rp.header().garbage() > 0, "garbage accumulated");
            int freeBefore = rp.freeSpace();

            reorganizer.reorganize(rp);

            assertEquals(0, rp.header().garbage(), "garbage reclaimed");
            assertEquals(0, rp.header().free(), "free list cleared");
            assertTrue(rp.freeSpace() > freeBefore, "compaction grew FreeSpace");
            assertEquals(List.of(1L, 2L, 4L, 5L, 7L, 8L, 10L, 11L, 12L), idsInOrder(rp, schema), "key order kept");
            assertEquals(9, rp.header().nRecs());
            for (int i : new int[]{1, 2, 4, 5, 7, 8, 10, 11, 12}) {
                assertTrue(search.findEqual(rp, k(i), kd, schema).isPresent(), "id " + i);
            }
            // 目录不变量。
            RecordPageDirectory d = rp.directory();
            assertEquals(1, rp.recordHeaderAt(d.slot(0)).nOwned());
            for (int i = 1; i < d.slotCount() - 1; i++) {
                int owned = rp.recordHeaderAt(d.slot(i)).nOwned();
                assertTrue(owned >= RecordPageInserter.MIN_N_OWNED && owned <= RecordPageInserter.MAX_N_OWNED);
            }
        });
    }

    /**
     * 验证 {@code reorganizeKeepsDeleteMarkedRecords} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void reorganizeKeepsDeleteMarkedRecords() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 5; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            int off3 = search.findEqual(rp, k(3), kd, schema).getAsInt();
            deleter.deleteMark(rp, off3); // delete-mark 但不 purge

            reorganizer.reorganize(rp);

            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), idsInOrder(rp, schema), "delete-marked kept in chain");
            assertEquals(5, rp.header().nRecs(), "delete-marked counted");
            int newOff3 = search.findEqual(rp, k(3), kd, schema).getAsInt();
            assertTrue(new RecordCursor(rp, newOff3, schema, registry).isDeleted(), "delete flag preserved");
        });
    }

    /**
     * 验证 {@code reorganizeDensifiesHeapNo} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void reorganizeDensifiesHeapNo() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 4; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            reorganizer.reorganize(rp);
            // 重排后 heapNo 稠密 2,3,4,5（infimum=0,supremum=1）。
            List<Integer> heapNos = new ArrayList<>();
            for (int off : rp.recordOffsetsInOrder()) {
                heapNos.add(rp.recordHeaderAt(off).heapNo());
            }
            assertEquals(List.of(2, 3, 4, 5), heapNos);
        });
    }
}
