package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.redo.FspPageFreeRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LOB 页链与 FSP/MTR/redo 协作测试。 */
class LobStorageTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(17);

    @TempDir
    Path dir;

    @Test
    void writesReadsAndFreesMultiPageTextChainWithGenericRedo() {
        try (Fixture fixture = new Fixture(dir.resolve("lob.ibd"))) {
            SegmentRef segment = fixture.createSegment(SegmentPurpose.LOB);
            String payload = "数".repeat(LobPageLayout.payloadCapacity(PAGE_SIZE) + 100);

            MiniTransaction write = fixture.manager.begin();
            ColumnValue.ExternalValue external = fixture.storage.write(write, segment,
                    ColumnType.longText(false), new ColumnValue.StringValue(payload));
            fixture.manager.commit(write);

            assertTrue(external.reference().pageCount() >= 3);
            assertTrue(fixture.manager.redoLogManager().bufferedRecords().stream()
                    .anyMatch(record -> record instanceof PageInitRecord init
                            && init.pageType() == PageType.BLOB));

            MiniTransaction read = fixture.manager.beginReadOnly();
            assertEquals(new ColumnValue.StringValue(payload), fixture.storage.read(
                    read, ColumnType.longText(false), external));
            fixture.manager.commit(read);

            MiniTransaction free = fixture.manager.begin();
            fixture.storage.free(free, segment, ColumnType.longText(false), external);
            fixture.manager.commit(free);
            assertTrue(fixture.manager.redoLogManager().bufferedRecords().stream()
                    .anyMatch(FspPageFreeRecord.class::isInstance));

            MiniTransaction staleRead = fixture.manager.beginReadOnly();
            assertThrows(LobPageCorruptedException.class, () -> fixture.storage.read(
                    staleRead, ColumnType.longText(false), external));
            fixture.manager.rollbackUncommitted(staleRead);
        }
    }

    @Test
    void rejectsWrongSegmentPurposeBeforeAllocatingPages() {
        try (Fixture fixture = new Fixture(dir.resolve("wrong-purpose.ibd"))) {
            SegmentRef segment = fixture.createSegment(SegmentPurpose.INDEX_LEAF);
            MiniTransaction write = fixture.manager.begin();
            assertThrows(LobSegmentMismatchException.class, () -> fixture.storage.write(write, segment,
                    ColumnType.longBlob(false), new ColumnValue.BinaryValue(new byte[1024])));
            fixture.manager.rollbackUncommitted(write);
            assertTrue(fixture.manager.redoLogManager().bufferedRecords().stream()
                    .noneMatch(record -> record instanceof PageInitRecord init
                            && init.pageType() == PageType.BLOB),
                    "wrong segment must fail before reserve/allocation publishes a BLOB page");
        }
    }

    @Test
    void wholeValueCrcDetectsPayloadCorruption() {
        try (Fixture fixture = new Fixture(dir.resolve("crc.ibd"))) {
            SegmentRef segment = fixture.createSegment(SegmentPurpose.LOB);
            MiniTransaction write = fixture.manager.begin();
            ColumnValue.ExternalValue external = fixture.storage.write(write, segment,
                    ColumnType.longBlob(false), new ColumnValue.BinaryValue(new byte[1024]));
            fixture.manager.commit(write);

            PageId first = PageId.of(external.reference().spaceId(), external.reference().firstPageNo());
            MiniTransaction corrupt = fixture.manager.begin();
            PageGuard guard = corrupt.getPage(fixture.pool, first, PageLatchMode.EXCLUSIVE);
            guard.writeBytes(LobPageLayout.DATA, new byte[]{1});
            fixture.manager.commit(corrupt);

            MiniTransaction read = fixture.manager.beginReadOnly();
            assertThrows(LobPageCorruptedException.class, () -> fixture.storage.read(
                    read, ColumnType.longBlob(false), external));
            fixture.manager.rollbackUncommitted(read);
        }
    }

    private final class Fixture implements AutoCloseable {
        private final PageStore store = new FileChannelPageStore();
        private final BufferPool pool = new LruBufferPool(store, PAGE_SIZE, 64);
        private final DiskSpaceManager disk = new DiskSpaceManager(pool, store, PAGE_SIZE);
        private final MiniTransactionManager manager = new MiniTransactionManager();
        private final LobStorage storage = new LobStorage(disk, pool, PAGE_SIZE, new TypeCodecRegistry());

        private Fixture(Path path) {
            MiniTransaction create = manager.begin();
            disk.createTablespace(create, SPACE, path, PageNo.of(128));
            manager.commit(create);
        }

        private SegmentRef createSegment(SegmentPurpose purpose) {
            MiniTransaction create = manager.begin();
            SegmentRef segment = disk.createSegment(create, SPACE, purpose);
            manager.commit(create);
            return segment;
        }

        @Override
        public void close() {
            pool.close();
            store.close();
        }
    }
}
