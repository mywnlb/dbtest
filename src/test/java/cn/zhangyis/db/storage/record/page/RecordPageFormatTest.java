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
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 空页初始化：header、infimum/supremum 系统记录、初始 2 槽目录、heapTop/freeSpace、空链。 */
class RecordPageFormatTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void formatBuildsEmptyIndexPage() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);

                IndexPageHeader h = rp.header();
                assertEquals(2, h.nDirSlots());
                assertEquals(98, h.heapTop());
                assertEquals(2, h.nHeap());
                assertEquals(0, h.nRecs());
                assertEquals(0, h.level());
                assertEquals(7L, h.indexId());
                assertEquals(IndexPageDirection.NO_DIRECTION, h.direction());

                RecordHeader inf = rp.recordHeaderAt(rp.infimumOffset());
                assertEquals(RecordType.INFIMUM, inf.recordType());
                assertEquals(0, inf.heapNo());
                assertEquals(1, inf.nOwned());
                assertEquals(82, inf.nextRecordOffset());
                assertEquals(16, inf.recordLength());
                assertArrayEquals(IndexPageLayout.INFIMUM_LABEL, rp.systemLabelAt(rp.infimumOffset()));

                RecordHeader sup = rp.recordHeaderAt(rp.supremumOffset());
                assertEquals(RecordType.SUPREMUM, sup.recordType());
                assertEquals(1, sup.heapNo());
                assertEquals(1, sup.nOwned());
                assertEquals(0, sup.nextRecordOffset());
                assertArrayEquals(IndexPageLayout.SUPREMUM_LABEL, rp.systemLabelAt(rp.supremumOffset()));

                RecordPageDirectory d = new RecordPageDirectory(g, PS);
                assertEquals(2, d.slotCount());
                assertEquals(66, d.slot(0));
                assertEquals(82, d.slot(1));

                // dirStart = 16384 - 8 - 2*2 = 16372；freeSpace = 16372 - 98 = 16274。
                assertEquals(16372, rp.dirStart());
                assertEquals(16274, rp.freeSpace());
                assertTrue(rp.recordOffsetsInOrder().isEmpty());
                assertEquals(List.of(), rp.recordOffsetsInOrder());

                RecordPageStructureSnapshot snapshot = rp.structureSnapshot();
                assertEquals(0, snapshot.level());
                assertEquals(0, snapshot.userRecordCount());
                assertEquals(38, snapshot.headerOffset());
                assertEquals(28, snapshot.headerImage().length);
                assertEquals(66, snapshot.heapOffset());
                assertEquals(32, snapshot.heapImage().length);
                assertEquals(16372, snapshot.directoryOffset());
                assertEquals(4, snapshot.directoryImage().length);
            }
        }
    }
}
