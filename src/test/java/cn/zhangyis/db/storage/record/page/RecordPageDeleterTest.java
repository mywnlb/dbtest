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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RecordPageDeleter delete-mark：标记后仍在链/可查、isDeleted、nRecs 不变；重复/系统记录拒绝。 */
class RecordPageDeleterTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);
    private final RecordPageSearch search = new RecordPageSearch(registry);
    private final RecordPageDeleter deleter = new RecordPageDeleter();

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
    void deleteMarkKeepsRecordInChainAndQueryable() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, "n10"), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, "n20"), kd, schema).pageOffset();
            inserter.insert(rp, PAGE, row(30, "n30"), kd, schema);
            int before = rp.header().nRecs();

            deleter.deleteMark(rp, off);

            RecordCursor c = new RecordCursor(rp, off, schema, registry);
            assertTrue(c.isDeleted(), "flag set");
            assertTrue(search.findEqual(rp, k(20), kd, schema).isPresent(), "still findable");
            assertTrue(rp.recordOffsetsInOrder().contains(off), "still in chain");
            assertEquals(before, rp.header().nRecs(), "nRecs unchanged (delete-marked counted)");
        });
    }

    @Test
    void rejectsDoubleDeleteMarkAndSystemRecord() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            int off = inserter.insert(rp, PAGE, row(1, "a"), kd, schema).pageOffset();
            deleter.deleteMark(rp, off);
            assertThrows(DatabaseValidationException.class, () -> deleter.deleteMark(rp, off), "double delete-mark");
            assertThrows(DatabaseValidationException.class,
                    () -> deleter.deleteMark(rp, rp.supremumOffset()), "system record");
            // setDeleted(false) 复位后可再次 mark（验证位级 toggle 保留 recordType）。
            rp.setDeleted(off, false);
            assertFalse(new RecordCursor(rp, off, schema, registry).isDeleted());
            assertEquals(RecordType.CONVENTIONAL, new RecordCursor(rp, off, schema, registry).recordType());
        });
    }
}
