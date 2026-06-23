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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RecordComparator 保序比较：单/复合 key、ASC/DESC、NULL 序、前缀 key、infimum/supremum 哨兵。
 * 比较走编码切片（复用 codec.compare），断言只看符号（&lt;0/0/&gt;0），不依赖具体 magnitude。
 */
class RecordComparatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordComparator comparator = new RecordComparator(registry);

    /** id INT(not null) + name VARCHAR(20)(nullable) 两列表，覆盖定长/变长 + NULL。 */
    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    private static IndexKeyDef keyDef(KeyOrder idOrder, int... cols) {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), idOrder, 0)));
    }

    private static IndexKeyDef compositeKeyDef(KeyOrder idOrder, KeyOrder nameOrder) {
        return new IndexKeyDef(7L, List.of(
                new KeyPartDef(new ColumnId(0), idOrder, 0),
                new KeyPartDef(new ColumnId(1), nameOrder, 0)));
    }

    private static SearchKey key(ColumnValue... vs) {
        return new SearchKey(List.of(vs));
    }

    /** 在页上放一条记录并返回其字段级游标。 */
    private RecordCursor place(RecordPage rp, TableSchema schema, long id, ColumnValue name) {
        LogicalRecord logical = new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), name),
                false, RecordType.CONVENTIONAL);
        byte[] bytes = new RecordEncoder(registry).encode(logical, schema);
        int heapNo = rp.nextHeapNo();
        int off = rp.allocateFromFreeSpace(bytes.length);
        rp.writeRecordBytes(off, bytes);
        rp.setHeapNo(off, heapNo);
        return new RecordCursor(rp, off, schema, registry);
    }

    private interface PageBody {
        void run(RecordPage rp, TableSchema schema);
    }

    /** 建一页空 INDEX 页并交给回调操作（统一管理 store/pool/guard 生命周期）。 */
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
    void singleKeyIntOrderAsc() {
        onPage((rp, schema) -> {
            RecordCursor rec = place(rp, schema, 5, new ColumnValue.StringValue("x"));
            IndexKeyDef kd = keyDef(KeyOrder.ASC);
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(3)), kd, schema) > 0,
                    "record 5 > key 3");
            assertEquals(0, comparator.compare(rec, key(new ColumnValue.IntValue(5)), kd, schema),
                    "record 5 == key 5");
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(9)), kd, schema) < 0,
                    "record 5 < key 9");
        });
    }

    @Test
    void descReversesOrder() {
        onPage((rp, schema) -> {
            RecordCursor rec = place(rp, schema, 5, new ColumnValue.StringValue("x"));
            IndexKeyDef kd = keyDef(KeyOrder.DESC);
            // 自然序 record 5 > key 3，DESC 取反 → record 在后 → 负。
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(3)), kd, schema) < 0);
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(9)), kd, schema) > 0);
            assertEquals(0, comparator.compare(rec, key(new ColumnValue.IntValue(5)), kd, schema));
        });
    }

    @Test
    void nullOrdersBeforeNonNull() {
        onPage((rp, schema) -> {
            RecordCursor nullName = place(rp, schema, 5, ColumnValue.NullValue.INSTANCE);
            RecordCursor realName = place(rp, schema, 5, new ColumnValue.StringValue("a"));
            IndexKeyDef asc = compositeKeyDef(KeyOrder.ASC, KeyOrder.ASC);
            IndexKeyDef descName = compositeKeyDef(KeyOrder.ASC, KeyOrder.DESC);
            SearchKey keyReal = key(new ColumnValue.IntValue(5), new ColumnValue.StringValue("a"));
            SearchKey keyNull = key(new ColumnValue.IntValue(5), ColumnValue.NullValue.INSTANCE);
            // ASC：record NULL < key 非 NULL → 负；DESC：取反 → 正。
            assertTrue(comparator.compare(nullName, keyReal, asc, schema) < 0);
            assertTrue(comparator.compare(nullName, keyReal, descName, schema) > 0);
            // record 非 NULL vs key NULL（ASC）→ 正。
            assertTrue(comparator.compare(realName, keyNull, asc, schema) > 0);
            // 两侧皆 NULL → 该 part 相等 → 整体 0。
            assertEquals(0, comparator.compare(nullName, keyNull, asc, schema));
        });
    }

    @Test
    void compositeKeyComparesPartsInOrder() {
        onPage((rp, schema) -> {
            RecordCursor rec = place(rp, schema, 5, new ColumnValue.StringValue("b"));
            IndexKeyDef kd = compositeKeyDef(KeyOrder.ASC, KeyOrder.ASC);
            // 第一 part 相等 → 看第二 part："b" > "a" 正，==  0，< "c" 负。
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(5), new ColumnValue.StringValue("a")), kd, schema) > 0);
            assertEquals(0, comparator.compare(rec, key(new ColumnValue.IntValue(5), new ColumnValue.StringValue("b")), kd, schema));
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(5), new ColumnValue.StringValue("c")), kd, schema) < 0);
            // 第一 part 不等 → 不看第二 part。
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(4), new ColumnValue.StringValue("z")), kd, schema) > 0);
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(6), new ColumnValue.StringValue("a")), kd, schema) < 0);
        });
    }

    @Test
    void prefixKeyMatchesOnProvidedParts() {
        onPage((rp, schema) -> {
            RecordCursor rec = place(rp, schema, 5, new ColumnValue.StringValue("whatever"));
            IndexKeyDef kd = compositeKeyDef(KeyOrder.ASC, KeyOrder.ASC);
            // 只给第一 part：前缀匹配 → 0（不看 name）。
            assertEquals(0, comparator.compare(rec, key(new ColumnValue.IntValue(5)), kd, schema));
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(6)), kd, schema) < 0);
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(4)), kd, schema) > 0);
        });
    }

    @Test
    void varcharLexicographicOrder() {
        onPage((rp, schema) -> {
            RecordCursor rec = place(rp, schema, 1, new ColumnValue.StringValue("apple"));
            IndexKeyDef kd = compositeKeyDef(KeyOrder.ASC, KeyOrder.ASC);
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(1), new ColumnValue.StringValue("banana")), kd, schema) < 0);
            assertTrue(comparator.compare(rec, key(new ColumnValue.IntValue(1), new ColumnValue.StringValue("aardvark")), kd, schema) > 0);
        });
    }

    @Test
    void infimumAndSupremumAreSentinels() {
        onPage((rp, schema) -> {
            RecordCursor inf = new RecordCursor(rp, rp.infimumOffset(), schema, registry);
            RecordCursor sup = new RecordCursor(rp, rp.supremumOffset(), schema, registry);
            IndexKeyDef kd = keyDef(KeyOrder.ASC);
            SearchKey anyKey = key(new ColumnValue.IntValue(42));
            assertEquals(RecordType.INFIMUM, inf.recordType());
            assertEquals(RecordType.SUPREMUM, sup.recordType());
            assertTrue(comparator.compare(inf, anyKey, kd, schema) < 0, "infimum < any key");
            assertTrue(comparator.compare(sup, anyKey, kd, schema) > 0, "supremum > any key");
        });
    }
}
