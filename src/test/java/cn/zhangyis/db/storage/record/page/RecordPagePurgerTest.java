package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RecordPagePurger：摘链 + n_owned/目录维护 + 组合并 + 空间回收；前置校验；purge 后空间被复用。 */
class RecordPagePurgerTest {

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

    @Test
    void purgeInteriorRecordUnlinksAndReclaims() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, "n10"), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, "n20"), kd, schema).pageOffset();
            inserter.insert(rp, PAGE, row(30, "n30"), kd, schema);
            int len = rp.recordHeaderAt(off).recordLength();
            int nRecsBefore = rp.header().nRecs();

            deleter.deleteMark(rp, off);
            purger.purge(rp, off);

            assertFalse(rp.recordOffsetsInOrder().contains(off), "unlinked from chain");
            assertTrue(search.findEqual(rp, k(20), kd, schema).isEmpty(), "no longer found");
            assertTrue(search.findEqual(rp, k(10), kd, schema).isPresent(), "others intact");
            assertTrue(search.findEqual(rp, k(30), kd, schema).isPresent());
            assertEquals(nRecsBefore - 1, rp.header().nRecs(), "nRecs--");
            assertEquals(len, rp.header().garbage(), "space into GarbageList");
        });
    }

    @Test
    void purgedSpaceIsReusedByInsert() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, "n10"), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, "same-length-name-x"), kd, schema).pageOffset();
            deleter.deleteMark(rp, off);
            purger.purge(rp, off);

            // 插入一条 ≤ 被回收容量的记录 → 复用该偏移。
            RecordRef ref = inserter.insert(rp, PAGE, row(25, "n25"), kd, schema);
            assertEquals(off, ref.pageOffset(), "reuses purged fragment");
            assertEquals(0, rp.header().garbage(), "garbage reclaimed on reuse");
        });
    }

    @Test
    void purgeGroupEndRepointsSlot() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            // 插 8 条触发一次 split：产生一个中间槽（组末记录 = 第 4 条 id=4）。
            for (int i = 1; i <= 8; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            int slotCountBefore = rp.directory().slotCount();
            assertTrue(slotCountBefore >= 3, "split produced a middle slot");
            // 找到中间槽 owner（slot(1)）记录并 purge 它（owner==target 路径）。
            int owner = rp.directory().slot(1);
            deleter.deleteMark(rp, owner);
            purger.purge(rp, owner);

            assertFalse(rp.recordOffsetsInOrder().contains(owner), "owner unlinked");
            assertEquals(7, rp.recordOffsetsInOrder().size(), "one record removed");
            // 目录仍自洽：slot(0)=infimum 仍 owns 1，中间/尾组 n_owned 合法。
            RecordPageDirectory d = rp.directory();
            assertEquals(1, rp.recordHeaderAt(d.slot(0)).nOwned());
            for (int i = 1; i < d.slotCount() - 1; i++) {
                int owned = rp.recordHeaderAt(d.slot(i)).nOwned();
                assertTrue(owned >= 1 && owned <= RecordPageInserter.MAX_N_OWNED, "slot " + i + " owned=" + owned);
            }
            // 链按 key 升序未乱（逐 key 存在性见 purgeManyKeepsInvariantsAndMerges）。
            long prev = Long.MIN_VALUE;
            for (int off : rp.recordOffsetsInOrder()) {
                long id = ((ColumnValue.IntValue) new RecordCursor(rp, off, schema, registry)
                        .readColumn(new ColumnId(0))).value();
                assertTrue(id > prev, "ascending after purge");
                prev = id;
            }
        });
    }

    @Test
    void purgeManyKeepsInvariantsAndMerges() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 20; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            // delete-mark + purge 偶数 id（约一半），逼组变小触发合并。
            for (int i = 2; i <= 20; i += 2) {
                int off = search.findEqual(rp, k(i), kd, schema).getAsInt();
                deleter.deleteMark(rp, off);
                purger.purge(rp, off);
            }
            // 奇数仍在、偶数已无。
            for (int i = 1; i <= 20; i++) {
                boolean present = search.findEqual(rp, k(i), kd, schema).isPresent();
                assertEquals(i % 2 == 1, present, "id " + i);
            }
            assertEquals(10, rp.header().nRecs());
            // 目录不变量。
            RecordPageDirectory d = rp.directory();
            assertEquals(1, rp.recordHeaderAt(d.slot(0)).nOwned(), "infimum owns 1");
            for (int i = 1; i < d.slotCount() - 1; i++) {
                int owned = rp.recordHeaderAt(d.slot(i)).nOwned();
                assertTrue(owned >= 1 && owned <= RecordPageInserter.MAX_N_OWNED, "slot " + i + " owned=" + owned);
            }
            int sup = rp.recordHeaderAt(d.slot(d.slotCount() - 1)).nOwned();
            assertTrue(sup >= 1 && sup <= RecordPageInserter.MAX_N_OWNED);
        });
    }

    @Test
    void rejectsNonDeletedAndSystemRecord() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            int off = inserter.insert(rp, PAGE, row(1, "a"), kd, schema).pageOffset();
            assertThrows(DatabaseValidationException.class, () -> purger.purge(rp, off), "not delete-marked");
            assertThrows(DatabaseValidationException.class,
                    () -> purger.purge(rp, rp.supremumOffset()), "system record");
        });
    }
}
