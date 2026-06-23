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
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RecordPageInserter key 有序插入：顺序/随机插入后链按 key 升序；group split 后 PageDirectory ownership 合法；
 * 页满抛 overflow；NULL key + 变长列插入与查回。经真实 PageGuard（16KB 页）。
 */
class RecordPageInserterTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);
    private final RecordPageSearch search = new RecordPageSearch(registry);

    /** id INT(not null) + name VARCHAR(20)(nullable)。 */
    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    /** name 用大 varchar，便于少量插入即填满页测 overflow。 */
    private static TableSchema bigSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(9000, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static IndexKeyDef nameKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0)));
    }

    private static SearchKey kId(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static SearchKey kName(ColumnValue name) {
        return new SearchKey(List.of(name));
    }

    private static LogicalRecord row(long id, ColumnValue name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), name), false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord row(long id, String name) {
        return row(id, new ColumnValue.StringValue(name));
    }

    /** 按链顺序读各记录的 id，顺带断言严格升序。 */
    private List<Long> idsInChainOrder(RecordPage rp, TableSchema schema) {
        List<Long> ids = new ArrayList<>();
        long prev = Long.MIN_VALUE;
        for (int off : rp.recordOffsetsInOrder()) {
            RecordCursor c = new RecordCursor(rp, off, schema, registry);
            long id = ((ColumnValue.IntValue) c.readColumn(new ColumnId(0))).value();
            assertTrue(id > prev, "ids must be strictly ascending, got " + id + " after " + prev);
            prev = id;
            ids.add(id);
        }
        return ids;
    }

    private interface PageBody {
        void run(RecordPage rp, TableSchema schema);
    }

    private void onPage(TableSchema schema, PageBody body) {
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

    @Test
    void sequentialInsertKeepsKeyOrder() {
        onPage(schema(), (rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 10; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            List<Long> ids = idsInChainOrder(rp, schema);
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), ids);
            assertEquals(10, rp.header().nRecs());
        });
    }

    @Test
    void randomInsertKeepsKeyOrder() {
        onPage(schema(), (rp, schema) -> {
            IndexKeyDef kd = idKey();
            List<Integer> order = new ArrayList<>();
            for (int i = 1; i <= 50; i++) {
                order.add(i);
            }
            Collections.shuffle(order, new Random(20260615L));
            for (int id : order) {
                inserter.insert(rp, PAGE, row(id, "n" + id), kd, schema);
            }
            List<Long> ids = idsInChainOrder(rp, schema);
            List<Long> expected = new ArrayList<>();
            for (long i = 1; i <= 50; i++) {
                expected.add(i);
            }
            assertEquals(expected, ids);
            assertEquals(50, rp.header().nRecs());
            // 每个 key 都可查回。
            for (long id = 1; id <= 50; id++) {
                assertTrue(search.findEqual(rp, kId(id), kd, schema).isPresent(), "missing id " + id);
            }
        });
    }

    @Test
    void groupSplitMaintainsOwnership() {
        onPage(schema(), (rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 30; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            RecordPageDirectory d = rp.directory();
            int n = d.slotCount();
            assertTrue(n > 2, "directory should have grown via split, slotCount=" + n);
            // infimum 槽恒 owns 1（自身）。
            assertEquals(1, rp.recordHeaderAt(d.slot(0)).nOwned(), "infimum slot owns exactly 1");
            // 中间槽（组末记录）n_owned ∈ [MIN..MAX]。
            for (int i = 1; i < n - 1; i++) {
                int owned = rp.recordHeaderAt(d.slot(i)).nOwned();
                assertTrue(owned >= RecordPageInserter.MIN_N_OWNED && owned <= RecordPageInserter.MAX_N_OWNED,
                        "middle slot " + i + " n_owned=" + owned);
            }
            // supremum 槽含自身，∈ [1..MAX]。
            int sup = rp.recordHeaderAt(d.slot(n - 1)).nOwned();
            assertTrue(sup >= 1 && sup <= RecordPageInserter.MAX_N_OWNED, "supremum slot n_owned=" + sup);
            // 全 key 仍可查回。
            for (long id = 1; id <= 30; id++) {
                assertTrue(search.findEqual(rp, kId(id), kd, schema).isPresent(), "missing id " + id);
            }
            assertEquals(30, rp.header().nRecs());
        });
    }

    @Test
    void pageFullThrowsOverflow() {
        onPage(bigSchema(), (rp, schema) -> {
            IndexKeyDef kd = idKey();
            String big = "x".repeat(8000);
            assertThrows(RecordPageOverflowException.class, () -> {
                for (int i = 0; i < 20; i++) {
                    inserter.insert(rp, PAGE, row(i, big), kd, schema);
                }
            });
        });
    }

    @Test
    void nullKeyAndVarlenInsertAndLookup() {
        onPage(schema(), (rp, schema) -> {
            IndexKeyDef kd = nameKey();
            // 乱序插入：name = "b", null, "a" → 链应为 null, "a", "b"（ASC，NULL 最小）。
            inserter.insert(rp, PAGE, row(30, "b"), kd, schema);
            inserter.insert(rp, PAGE, row(10, ColumnValue.NullValue.INSTANCE), kd, schema);
            inserter.insert(rp, PAGE, row(20, "a"), kd, schema);

            assertEquals(List.of(10L, 20L, 30L), idsInChainOrder(rp, schema), "null-name row sorts first");

            RecordCursor nullRow = search.findEqualCursor(rp, kName(ColumnValue.NullValue.INSTANCE), kd, schema);
            assertTrue(nullRow.isNull(new ColumnId(1)));
            assertEquals(new ColumnValue.IntValue(10), nullRow.readColumn(new ColumnId(0)));

            RecordCursor aRow = search.findEqualCursor(rp, kName(new ColumnValue.StringValue("a")), kd, schema);
            assertEquals(new ColumnValue.IntValue(20), aRow.readColumn(new ColumnId(0)));
        });
    }

    @Test
    void insertReusesFreedGarbageFragment() {
        onPage(schema(), (rp, schema) -> {
            IndexKeyDef kd = idKey();
            // 在 heap 切一块"碎片"（不串入用户链），再 free 入 GarbageList。
            LogicalRecord throwaway = row(999, "reuse-fragment");
            byte[] bytes = new cn.zhangyis.db.storage.record.format.RecordEncoder(registry).encode(throwaway, schema);
            int fragHeapNo = rp.nextHeapNo();
            int frag = rp.allocateFromFreeSpace(bytes.length);
            rp.writeRecordBytes(frag, bytes);
            rp.setHeapNo(frag, fragHeapNo);
            new HeapSpaceManager(rp).free(frag);

            // 插入一条 ≤ 碎片容量的记录：应复用 frag 偏移与其 heapNo。
            RecordRef ref = inserter.insert(rp, PAGE, row(5, "n5"), kd, schema);
            assertEquals(frag, ref.pageOffset(), "insert reuses freed fragment offset");
            assertEquals(fragHeapNo, ref.heapNo(), "insert reuses freed fragment heapNo");
            assertEquals(0, rp.header().garbage(), "garbage reclaimed");
            assertTrue(search.findEqual(rp, kId(5), kd, schema).isPresent(), "reused record findable");
        });
    }
}
