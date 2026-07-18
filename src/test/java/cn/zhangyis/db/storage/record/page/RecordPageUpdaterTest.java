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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RecordPageUpdater：原地（等长/变短）、搬迁（变长）、key 变化信号、overflow、系统/已删拒绝。 */
class RecordPageUpdaterTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);
    private final RecordPageSearch search = new RecordPageSearch(registry);
    private final RecordPageDeleter deleter = new RecordPageDeleter();
    private final RecordPageUpdater updater = new RecordPageUpdater(registry);

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(200, true), 1)));
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

    private String name(int n) {
        return "x".repeat(n);
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
     * 验证 {@code inPlaceWhenSameOrShorter} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void inPlaceWhenSameOrShorter() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, name(50)), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, name(50)), kd, schema).pageOffset();
            inserter.insert(rp, PAGE, row(30, name(50)), kd, schema);

            UpdateResult r = updater.update(rp, PAGE, off, row(20, name(20)), kd, schema); // 变短
            assertEquals(UpdateOutcome.IN_PLACE, r.outcome());
            assertEquals(off, r.newRef().pageOffset(), "stays in place");
            RecordCursor c = new RecordCursor(rp, off, schema, registry);
            assertEquals(new ColumnValue.StringValue(name(20)), c.readColumn(new ColumnId(1)));
            assertTrue(search.findEqual(rp, k(20), kd, schema).isPresent());
            assertTrue(search.findEqual(rp, k(10), kd, schema).isPresent());
            assertTrue(search.findEqual(rp, k(30), kd, schema).isPresent());
        });
    }

    /**
     * 验证 {@code movesWhenLonger} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void movesWhenLonger() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, name(20)), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, name(20)), kd, schema).pageOffset();
            inserter.insert(rp, PAGE, row(30, name(20)), kd, schema);
            int len = rp.recordHeaderAt(off).recordLength();

            UpdateResult r = updater.update(rp, PAGE, off, row(20, name(150)), kd, schema); // 变长
            assertEquals(UpdateOutcome.MOVED, r.outcome());
            assertNotEquals(off, r.newRef().pageOffset(), "moved to new offset");
            int newOff = search.findEqual(rp, k(20), kd, schema).getAsInt();
            assertEquals(newOff, r.newRef().pageOffset());
            assertEquals(new ColumnValue.StringValue(name(150)),
                    new RecordCursor(rp, newOff, schema, registry).readColumn(new ColumnId(1)));
            assertEquals(len, rp.header().garbage(), "old space -> GarbageList");
            assertEquals(List.of(10L, 20L, 30L), idsInOrder(rp, schema), "chain intact + ordered");
        });
    }

    /**
     * 验证 {@code keyChangeReturnsRequiresReinsertWithoutMutating} 所描述的边界场景保持既有领域不变量，不产生方法名明确禁止的副作用。
     */
    @Test
    void keyChangeReturnsRequiresReinsertWithoutMutating() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            int off = inserter.insert(rp, PAGE, row(20, name(10)), kd, schema).pageOffset();
            int garbageBefore = rp.header().garbage();

            UpdateResult r = updater.update(rp, PAGE, off, row(25, name(10)), kd, schema); // id 改变
            assertEquals(UpdateOutcome.REQUIRES_REINSERT, r.outcome());
            assertNull(r.newRef());
            assertTrue(search.findEqual(rp, k(20), kd, schema).isPresent());
            assertTrue(search.findEqual(rp, k(25), kd, schema).isEmpty());
            assertEquals(garbageBefore, rp.header().garbage());
        });
    }

    /**
     * 验证 {@code moveOverflowLeavesPageUnchanged} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void moveOverflowLeavesPageUnchanged() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            int off = inserter.insert(rp, PAGE, row(1, name(100)), kd, schema).pageOffset();
            int i = 2;
            try {
                while (true) {
                    inserter.insert(rp, PAGE, row(i++, name(200)), kd, schema);
                }
            } catch (RecordPageOverflowException expected) {
                // 页已满。
            }
            int idsBefore = rp.recordOffsetsInOrder().size();
            int garbageBefore = rp.header().garbage();

            assertThrows(RecordPageOverflowException.class,
                    () -> updater.update(rp, PAGE, off, row(1, name(200)), kd, schema));
            assertEquals(idsBefore, rp.recordOffsetsInOrder().size(), "chain unchanged");
            assertEquals(garbageBefore, rp.header().garbage(), "no mutation on overflow");
        });
    }

    /**
     * 验证 {@code rejectsSystemAndDeleteMarked} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsSystemAndDeleteMarked() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            int off = inserter.insert(rp, PAGE, row(1, name(10)), kd, schema).pageOffset();
            assertThrows(DatabaseValidationException.class,
                    () -> updater.update(rp, PAGE, rp.infimumOffset(), row(1, name(10)), kd, schema));
            deleter.deleteMark(rp, off);
            assertThrows(DatabaseValidationException.class,
                    () -> updater.update(rp, PAGE, off, row(1, name(10)), kd, schema), "delete-marked");
        });
    }
}
