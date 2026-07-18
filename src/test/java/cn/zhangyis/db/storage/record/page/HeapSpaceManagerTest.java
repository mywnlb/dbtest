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

/** HeapSpaceManager：free 入 GarbageList（GARBAGE+）、allocate first-fit 复用（沿用 heapNo、GARBAGE-整块）、容量不足回退 FreeSpace。 */
class HeapSpaceManagerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(50, true), 1)));
    }

    /** 直接在 heap 切一条编码记录（不串入用户链），返回 offset；模拟一块可被 free 的记录区。 */
    private int place(RecordPage rp, TableSchema schema, long id, String name) {
        LogicalRecord logical = new LogicalRecord(1, List.of(
                new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)), false, RecordType.CONVENTIONAL);
        byte[] bytes = new RecordEncoder(registry).encode(logical, schema);
        int heapNo = rp.nextHeapNo();
        int off = rp.allocateFromFreeSpace(bytes.length);
        rp.writeRecordBytes(off, bytes);
        rp.setHeapNo(off, heapNo);
        return off;
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

    /**
     * 验证 {@code freeThenAllocateReusesFragment} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void freeThenAllocateReusesFragment() {
        onPage((rp, schema) -> {
            int off = place(rp, schema, 1, "hello-world-payload");
            int heapNo = rp.recordHeaderAt(off).heapNo();
            int cap = rp.recordHeaderAt(off).recordLength();
            HeapSpaceManager heap = new HeapSpaceManager(rp);

            heap.free(off);
            assertEquals(cap, rp.header().garbage(), "garbage += capacity");
            assertEquals(off, rp.header().free(), "free head = fragment");

            HeapSpaceManager.Allocation a = heap.allocate(cap);
            assertTrue(a.reused(), "reused fragment");
            assertEquals(off, a.offset());
            assertEquals(heapNo, a.heapNo(), "reuses fragment heapNo");
            assertEquals(0, rp.header().garbage(), "garbage -= full capacity");
            assertEquals(0, rp.header().free(), "free list empty");
        });
    }

    /**
     * 验证 {@code allocateFallsBackToFreeSpaceWhenFragmentTooSmall} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void allocateFallsBackToFreeSpaceWhenFragmentTooSmall() {
        onPage((rp, schema) -> {
            int off = place(rp, schema, 1, "tiny");
            int cap = rp.recordHeaderAt(off).recordLength();
            HeapSpaceManager heap = new HeapSpaceManager(rp);
            heap.free(off);
            int garbageBefore = rp.header().garbage();
            int heapTopBefore = rp.header().heapTop();

            HeapSpaceManager.Allocation a = heap.allocate(cap + 100);
            assertFalse(a.reused(), "fragment too small -> FreeSpace");
            assertTrue(a.offset() >= heapTopBefore, "carved from FreeSpace (>= old heapTop)");
            assertEquals(garbageBefore, rp.header().garbage(), "garbage unchanged on fallback");
        });
    }

    /**
     * 验证 {@code firstFitPicksFirstFittingFragment} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void firstFitPicksFirstFittingFragment() {
        onPage((rp, schema) -> {
            int small = place(rp, schema, 1, "s");          // 容量较小
            int big = place(rp, schema, 2, "big-payload-here-x"); // 容量较大
            int bigCap = rp.recordHeaderAt(big).recordLength();
            HeapSpaceManager heap = new HeapSpaceManager(rp);
            heap.free(small); // FREE head = small
            heap.free(big);   // FREE head = big -> small

            // 需要 bigCap 字节：small 容量不足被跳过，命中 big。
            HeapSpaceManager.Allocation a = heap.allocate(bigCap);
            assertTrue(a.reused());
            assertEquals(big, a.offset(), "first-fit skips too-small head, picks big");
            // small 仍在 FREE 链上（big 被摘除后 FREE head 应回到 small）。
            assertEquals(small, rp.header().free());
        });
    }
}
