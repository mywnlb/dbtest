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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** PageDirectory 槽读写、插槽/删槽位移、越界与最小槽数、无空间插槽 overflow。 */
class RecordPageDirectoryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void readWriteSlots() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                new RecordPage(g, PS).format(1, 0);
                RecordPageDirectory d = new RecordPageDirectory(g, PS);
                assertEquals(2, d.slotCount());
                assertEquals(66, d.slot(0));
                assertEquals(82, d.slot(1));
                assertThrows(PageDirectoryCorruptedException.class, () -> d.slot(2));
                d.setSlot(1, 200);
                assertEquals(200, d.slot(1));
            }
        }
    }

    @Test
    void insertAndRemoveShiftSlots() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                new RecordPage(g, PS).format(1, 0);
                RecordPageDirectory d = new RecordPageDirectory(g, PS);
                d.insertSlot(1, 150);
                assertEquals(3, d.slotCount());
                assertEquals(66, d.slot(0));
                assertEquals(150, d.slot(1));
                assertEquals(82, d.slot(2)); // 原 supremum 槽逻辑右移

                d.removeSlot(1);
                assertEquals(2, d.slotCount());
                assertEquals(66, d.slot(0));
                assertEquals(82, d.slot(1));

                // 不得降到 2 槽以下。
                assertThrows(PageDirectoryCorruptedException.class, () -> d.removeSlot(1));
            }
        }
    }

    @Test
    void insertSlotRejectedWhenNoRoom() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(1, 0);
                rp.allocateFromFreeSpace(rp.freeSpace()); // heapTop 顶到 dirStart，free=0
                RecordPageDirectory d = new RecordPageDirectory(g, PS);
                assertThrows(RecordPageOverflowException.class, () -> d.insertSlot(1, 150));
            }
        }
    }
}
