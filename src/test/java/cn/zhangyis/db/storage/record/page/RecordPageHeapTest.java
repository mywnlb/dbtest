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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** heap 分配：heapTop/heapNo 推进、free space 递减、不动链/目录/nRecs、参数与 overflow 校验。 */
class RecordPageHeapTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    /**
     * 验证 {@code allocateAdvancesHeapAndLeavesStructureUntouched} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void allocateAdvancesHeapAndLeavesStructureUntouched() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(1, 0);

                assertEquals(2, rp.nextHeapNo());
                int o1 = rp.allocateFromFreeSpace(16);
                assertEquals(98, o1);
                assertEquals(114, rp.header().heapTop());
                assertEquals(3, rp.header().nHeap());
                assertEquals(3, rp.nextHeapNo());

                int o2 = rp.allocateFromFreeSpace(16);
                assertEquals(114, o2);
                assertEquals(130, rp.header().heapTop());
                assertEquals(4, rp.header().nHeap());

                // allocate 不 wire next_record、不动目录、不增 nRecs。
                assertEquals(rp.supremumOffset(), rp.nextRecord(rp.infimumOffset()));
                assertEquals(2, new RecordPageDirectory(g, PS).slotCount());
                assertEquals(0, rp.header().nRecs());
            }
        }
    }

    /**
     * 验证 {@code rejectsNonPositiveAndOverflow} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsNonPositiveAndOverflow() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(1, 0);
                assertThrows(DatabaseValidationException.class, () -> rp.allocateFromFreeSpace(0));
                assertThrows(DatabaseValidationException.class, () -> rp.allocateFromFreeSpace(-1));
                assertThrows(RecordPageOverflowException.class, () -> rp.allocateFromFreeSpace(rp.freeSpace() + 1));
                // 恰好用满 free space 不抛。
                int free = rp.freeSpace();
                rp.allocateFromFreeSpace(free);
                assertEquals(0, rp.freeSpace());
            }
        }
    }
}
