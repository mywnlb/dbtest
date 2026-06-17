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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** next_record 链：手工按序串接后遍历得逻辑序；成环 / 越界偏移判损坏。 */
class RecordPageChainTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void walkReturnsRecordsInChainOrder() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(1, 0);
                int o1 = rp.allocateFromFreeSpace(16);
                int o2 = rp.allocateFromFreeSpace(16);
                int o3 = rp.allocateFromFreeSpace(16);
                // 模拟 R4 insert 的串接：infimum -> o1 -> o2 -> o3 -> supremum。
                rp.setNextRecord(rp.infimumOffset(), o1);
                rp.setNextRecord(o1, o2);
                rp.setNextRecord(o2, o3);
                rp.setNextRecord(o3, rp.supremumOffset());

                assertEquals(List.of(o1, o2, o3), rp.recordOffsetsInOrder());
                assertEquals(o2, rp.nextRecord(o1));
            }
        }
    }

    @Test
    void cycleIsDetected() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(1, 0);
                int o1 = rp.allocateFromFreeSpace(16);
                int o2 = rp.allocateFromFreeSpace(16);
                int o3 = rp.allocateFromFreeSpace(16);
                rp.setNextRecord(rp.infimumOffset(), o1);
                rp.setNextRecord(o1, o2);
                rp.setNextRecord(o2, o3);
                rp.setNextRecord(o3, o1); // 成环，永不到 supremum
                assertThrows(PageDirectoryCorruptedException.class, rp::recordOffsetsInOrder);
            }
        }
    }

    @Test
    void outOfBodyOffsetIsDetected() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(1, 0);
                // 50 在 page header 区（< USER_RECORDS_START 98），属页体外的非法记录偏移。
                rp.setNextRecord(rp.infimumOffset(), 50);
                assertThrows(PageDirectoryCorruptedException.class, rp::recordOffsetsInOrder);
            }
        }
    }
}
