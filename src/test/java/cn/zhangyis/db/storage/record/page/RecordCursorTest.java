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
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RecordCursor 字段级读：手工放置 encoded 记录后，readColumn/columnSlice/isNull/isDeleted/materialize/heapNo 正确。 */
class RecordCursorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static ColumnDef col(int ordinal, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ordinal), name, type, ordinal);
    }

    @Test
    void readsFieldsFromPlacedRecord() {
        TableSchema schema = new TableSchema(1, List.of(
                col(0, "id", ColumnType.intType(false, false)),
                col(1, "name", ColumnType.varchar(20, true))));
        LogicalRecord logical = new LogicalRecord(1, List.of(
                new ColumnValue.IntValue(42), new ColumnValue.StringValue("héllo")),
                false, RecordType.CONVENTIONAL);

        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);

                byte[] bytes = new RecordEncoder(registry).encode(logical, schema);
                int heapNo = rp.nextHeapNo();
                int off = rp.allocateFromFreeSpace(bytes.length);
                rp.writeRecordBytes(off, bytes);
                rp.setHeapNo(off, heapNo);

                RecordCursor c = new RecordCursor(rp, off, schema, registry);
                assertEquals(heapNo, c.heapNo());
                assertEquals(2, heapNo);
                assertEquals(RecordType.CONVENTIONAL, c.recordType());
                assertFalse(c.isDeleted());
                assertEquals(new ColumnValue.IntValue(42), c.readColumn(new ColumnId(0)));
                assertEquals(new ColumnValue.StringValue("héllo"), c.readColumn(new ColumnId(1)));
                assertFalse(c.isNull(new ColumnId(1)));
                assertEquals(4, c.columnSlice(new ColumnId(0)).length());
                assertEquals(logical, c.materialize());
                assertEquals(off, c.recordRef(PageId.of(SPACE, PageNo.of(3)), 7L).pageOffset());
            }
        }
    }

    @Test
    void readsNullColumn() {
        TableSchema schema = new TableSchema(1, List.of(
                col(0, "id", ColumnType.intType(false, false)),
                col(1, "name", ColumnType.varchar(20, true))));
        LogicalRecord logical = new LogicalRecord(1, List.of(
                new ColumnValue.IntValue(1), ColumnValue.NullValue.INSTANCE),
                true, RecordType.CONVENTIONAL);

        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);
                byte[] bytes = new RecordEncoder(registry).encode(logical, schema);
                int off = rp.allocateFromFreeSpace(bytes.length);
                rp.writeRecordBytes(off, bytes);
                rp.setHeapNo(off, rp.nextHeapNo());

                RecordCursor c = new RecordCursor(rp, off, schema, registry);
                assertTrue(c.isDeleted());
                assertTrue(c.isNull(new ColumnId(1)));
                assertEquals(new ColumnValue.IntValue(1), c.readColumn(new ColumnId(0)));
            }
        }
    }
}
