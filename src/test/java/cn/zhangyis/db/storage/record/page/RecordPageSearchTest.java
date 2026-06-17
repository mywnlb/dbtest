package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
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
import java.util.List;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RecordPageSearch 页内查找：手工建按 key 升序串接的链 + 一个中间目录槽（强制走二分分支），
 * 验证 findEqual 命中/未命中/边界、findInsertPosition 在头/中/尾/重复 key 处、findEqualCursor 命中与抛错、空页。
 */
class RecordPageSearchTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageSearch search = new RecordPageSearch(registry);

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    /** 单列（id）升序 key 定义。 */
    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey k(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    /** 放一条记录，返回其页内偏移（heapNo 已正确回填）。 */
    private int place(RecordPage rp, TableSchema schema, long id, String name) {
        LogicalRecord logical = new LogicalRecord(1, List.of(
                new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
        byte[] bytes = new RecordEncoder(registry).encode(logical, schema);
        int heapNo = rp.nextHeapNo();
        int off = rp.allocateFromFreeSpace(bytes.length);
        rp.writeRecordBytes(off, bytes);
        rp.setHeapNo(off, heapNo);
        return off;
    }

    /**
     * 按给定 id 顺序放记录并 wire 成 infimum -&gt; ... -&gt; supremum 升序链；返回各记录偏移（与 ids 同序）。
     * 调用方需保证 ids 已升序（本 helper 不排序，只忠实串接以模拟有序页）。
     */
    private int[] buildChain(RecordPage rp, TableSchema schema, long... ids) {
        int[] offs = new int[ids.length];
        for (int i = 0; i < ids.length; i++) {
            offs[i] = place(rp, schema, ids[i], "n" + ids[i]);
        }
        int prev = rp.infimumOffset();
        for (int off : offs) {
            rp.setNextRecord(prev, off);
            prev = off;
        }
        rp.setNextRecord(prev, rp.supremumOffset());
        return offs;
    }

    private interface PageBody {
        void run(RecordPage rp, TableSchema schema);
    }

    private void onPage(PageBody body) {
        TableSchema schema = schema();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);
                body.run(rp, schema);
            }
        }
    }

    @Test
    void findEqualHitsAndMissesAcrossDirectorySlots() {
        onPage((rp, schema) -> {
            int[] offs = buildChain(rp, schema, 10, 20, 30, 40, 50);
            // 中间插一个目录槽指向 id=30，使 slotCount=3，强制 findEqual/InsertPosition 走二分分支。
            rp.directory().insertSlot(1, offs[2]);
            assertEquals(3, rp.directory().slotCount());
            IndexKeyDef kd = idKey();

            assertEquals(OptionalInt.of(offs[0]), search.findEqual(rp, k(10), kd, schema), "first");
            assertEquals(OptionalInt.of(offs[2]), search.findEqual(rp, k(30), kd, schema), "slot boundary");
            assertEquals(OptionalInt.of(offs[4]), search.findEqual(rp, k(50), kd, schema), "last");
            assertEquals(OptionalInt.of(offs[1]), search.findEqual(rp, k(20), kd, schema), "low group");
            assertEquals(OptionalInt.of(offs[3]), search.findEqual(rp, k(40), kd, schema), "high group");

            assertTrue(search.findEqual(rp, k(5), kd, schema).isEmpty(), "below min");
            assertTrue(search.findEqual(rp, k(25), kd, schema).isEmpty(), "gap low group");
            assertTrue(search.findEqual(rp, k(35), kd, schema).isEmpty(), "gap high group");
            assertTrue(search.findEqual(rp, k(55), kd, schema).isEmpty(), "above max");
        });
    }

    @Test
    void findInsertPositionHeadMidTail() {
        onPage((rp, schema) -> {
            int[] offs = buildChain(rp, schema, 10, 20, 30, 40, 50);
            rp.directory().insertSlot(1, offs[2]);
            IndexKeyDef kd = idKey();

            assertEquals(rp.infimumOffset(), search.findInsertPosition(rp, k(5), kd, schema), "before all -> infimum");
            assertEquals(offs[1], search.findInsertPosition(rp, k(25), kd, schema), "between 20 and 30 -> after 20");
            assertEquals(offs[2], search.findInsertPosition(rp, k(35), kd, schema), "between 30 and 40 -> after 30");
            assertEquals(offs[4], search.findInsertPosition(rp, k(55), kd, schema), "after all -> after 50");
        });
    }

    @Test
    void findInsertPositionAfterDuplicates() {
        onPage((rp, schema) -> {
            // 三条 id=20 重复键，findEqual 取首条，findInsertPosition 取末条之后（稳定插入）。
            int[] offs = buildChain(rp, schema, 10, 20, 20, 20, 30);
            IndexKeyDef kd = idKey();
            assertEquals(OptionalInt.of(offs[1]), search.findEqual(rp, k(20), kd, schema), "first duplicate");
            assertEquals(offs[3], search.findInsertPosition(rp, k(20), kd, schema), "after last duplicate");
        });
    }

    @Test
    void findEqualCursorReturnsRowOrThrows() {
        onPage((rp, schema) -> {
            buildChain(rp, schema, 10, 20, 30);
            IndexKeyDef kd = idKey();
            RecordCursor c = search.findEqualCursor(rp, k(20), kd, schema);
            assertEquals(new ColumnValue.IntValue(20), c.readColumn(new ColumnId(0)));
            assertEquals(new ColumnValue.StringValue("n20"), c.readColumn(new ColumnId(1)));
            assertFalse(c.isDeleted());
            assertThrows(RecordNotFoundException.class, () -> search.findEqualCursor(rp, k(99), kd, schema));
        });
    }

    @Test
    void emptyPageFindsNothing() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            assertTrue(search.findEqual(rp, k(1), kd, schema).isEmpty());
            assertEquals(rp.infimumOffset(), search.findInsertPosition(rp, k(1), kd, schema));
            assertThrows(RecordNotFoundException.class, () -> search.findEqualCursor(rp, k(1), kd, schema));
        });
    }
}
