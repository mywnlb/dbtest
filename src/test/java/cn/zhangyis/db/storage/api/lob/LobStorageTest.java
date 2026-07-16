package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
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
import cn.zhangyis.db.storage.record.type.LobCodec;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.record.type.UnsupportedColumnTypeException;
import cn.zhangyis.db.storage.redo.FspPageFreeRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

    /** 计划阶段只冻结纯值数据，不读取 inode/FSP；同输入结果确定且调用方数组变化不能污染 payload/prefix。 */
    @Test
    void plansWriteWithoutIoAndFreezesExactPayloadWorkload() {
        try (Fixture fixture = new Fixture(dir.resolve("plan.ibd"))) {
            SegmentRef wrongPurpose = fixture.createSegment(SegmentPurpose.INDEX_LEAF);
            byte[] raw = new byte[LobPageLayout.payloadCapacity(PAGE_SIZE) + 31];
            Arrays.fill(raw, (byte) 7);

            LobWritePlan first = fixture.storage.planWrite(wrongPurpose, ColumnType.longBlob(false),
                    new ColumnValue.BinaryValue(raw));
            LobWritePlan second = fixture.storage.planWrite(wrongPurpose, ColumnType.longBlob(false),
                    new ColumnValue.BinaryValue(raw));
            raw[0] = 99;
            CRC32 crc = new CRC32();
            crc.update(first.payload());

            assertEquals(2, first.pageCount());
            assertEquals(first.totalLength(), first.payload().length);
            assertEquals(crc.getValue(), first.crc32());
            assertEquals(fixture.storage.writeWorkload(first.totalLength()), first.workload());
            assertArrayEquals(first.payload(), second.payload());
            assertArrayEquals(first.inlinePrefix(), second.inlinePrefix());
            assertEquals(7, first.payload()[0]);
            assertTrue(fixture.manager.redoLogManager().bufferedRecords().stream()
                    .noneMatch(record -> record instanceof PageInitRecord init && init.pageType() == PageType.BLOB));
        }
    }

    /** inline/空值无需页链，非 LOB 或已有 external value 都必须在任何 IO 前拒绝。 */
    @Test
    void rejectsValuesThatCannotFormANewExternalLobPlan() {
        try (Fixture fixture = new Fixture(dir.resolve("invalid-plan.ibd"))) {
            SegmentRef segment = fixture.createSegment(SegmentPurpose.LOB);
            assertThrows(DatabaseValidationException.class, () -> fixture.storage.planWrite(segment,
                    ColumnType.longBlob(false), new ColumnValue.BinaryValue(new byte[0])));
            assertThrows(DatabaseValidationException.class, () -> fixture.storage.planWrite(segment,
                    ColumnType.longBlob(false), new ColumnValue.BinaryValue(
                            new byte[LobCodec.INLINE_PAYLOAD_LIMIT])));
            assertThrows(UnsupportedColumnTypeException.class, () -> fixture.storage.planWrite(segment,
                    ColumnType.intType(false, false), new ColumnValue.IntValue(1)));
        }
    }

    /** 未 transfer 的 guard close 在同一 active MTR 反序回收新页；重复 close 必须幂等。 */
    @Test
    void closeCompensatesUntransferredAllocationAndIsIdempotent() {
        try (Fixture fixture = new Fixture(dir.resolve("compensate.ibd"))) {
            SegmentRef segment = fixture.createSegment(SegmentPurpose.LOB);
            LobWritePlan plan = fixture.storage.planWrite(segment, ColumnType.longBlob(false),
                    new ColumnValue.BinaryValue(new byte[20_000]));
            MiniTransaction write = fixture.manager.begin();
            LobWriteAllocation allocation = fixture.storage.writePlanned(write, plan);
            ColumnValue.ExternalValue stale = allocation.value();

            allocation.close();
            allocation.close();
            fixture.manager.commit(write);

            MiniTransaction read = fixture.manager.beginReadOnly();
            assertThrows(LobPageCorruptedException.class,
                    () -> fixture.storage.read(read, ColumnType.longBlob(false), stale));
            fixture.manager.rollbackUncommitted(read);
        }
    }

    /** transfer 把页链交给即将发布的 undo/row；guard close 此后不能回收，链仍可正常读取。 */
    @Test
    void transferredAllocationSurvivesGuardClose() {
        try (Fixture fixture = new Fixture(dir.resolve("transfer.ibd"))) {
            SegmentRef segment = fixture.createSegment(SegmentPurpose.LOB);
            byte[] payload = new byte[20_000];
            Arrays.fill(payload, (byte) 3);
            MiniTransaction write = fixture.manager.begin();
            LobWriteAllocation allocation = fixture.storage.writePlanned(write,
                    fixture.storage.planWrite(segment, ColumnType.longBlob(false),
                            new ColumnValue.BinaryValue(payload)));
            ColumnValue.ExternalValue external = allocation.value();
            allocation.transferOwnership();
            allocation.close();
            fixture.manager.commit(write);

            MiniTransaction read = fixture.manager.beginReadOnly();
            ColumnValue.BinaryValue restored = (ColumnValue.BinaryValue) fixture.storage.read(
                    read, ColumnType.longBlob(false), external);
            assertArrayEquals(payload, restored.value());
            fixture.manager.commit(read);
        }
    }

    /** 后一项在 purpose preflight 失败时，前一 allocation 仍由其 guard 在同一 MTR 补偿。 */
    @Test
    void compensatesEarlierAllocationWhenLaterPlannedWriteFails() {
        try (Fixture fixture = new Fixture(dir.resolve("multi-failure.ibd"))) {
            SegmentRef lob = fixture.createSegment(SegmentPurpose.LOB);
            SegmentRef wrong = fixture.createSegment(SegmentPurpose.INDEX_LEAF);
            MiniTransaction write = fixture.manager.begin();
            LobWriteAllocation first = fixture.storage.writePlanned(write, fixture.storage.planWrite(lob,
                    ColumnType.longBlob(false), new ColumnValue.BinaryValue(new byte[20_000])));
            ColumnValue.ExternalValue stale = first.value();

            assertThrows(LobSegmentMismatchException.class, () -> fixture.storage.writePlanned(write,
                    fixture.storage.planWrite(wrong, ColumnType.longBlob(false),
                            new ColumnValue.BinaryValue(new byte[20_000]))));
            first.close();
            fixture.manager.commit(write);

            MiniTransaction read = fixture.manager.beginReadOnly();
            assertThrows(LobPageCorruptedException.class,
                    () -> fixture.storage.read(read, ColumnType.longBlob(false), stale));
            fixture.manager.rollbackUncommitted(read);
        }
    }

    /** guard 只能由创建线程在 ACTIVE MTR 内转移/补偿；终态 MTR 上 close 必须拒绝伪造成功。 */
    @Test
    void rejectsWrongThreadAndNonActiveMtrCompensation() throws Exception {
        try (Fixture fixture = new Fixture(dir.resolve("guard-owner.ibd"))) {
            SegmentRef segment = fixture.createSegment(SegmentPurpose.LOB);
            MiniTransaction write = fixture.manager.begin();
            LobWriteAllocation allocation = fixture.storage.writePlanned(write, fixture.storage.planWrite(segment,
                    ColumnType.longBlob(false), new ColumnValue.BinaryValue(new byte[20_000])));

            try (var executor = Executors.newSingleThreadExecutor()) {
                ExecutionException wrongThread = assertThrows(ExecutionException.class,
                        () -> executor.submit(allocation::close).get(5, TimeUnit.SECONDS));
                assertTrue(wrongThread.getCause() instanceof LobAllocationStateException);
            }
            fixture.manager.commit(write);
            LobAllocationStateException terminal = assertThrows(LobAllocationStateException.class,
                    allocation::close);
            assertTrue(terminal.getMessage().contains("COMMITTED"));

            MiniTransaction cleanup = fixture.manager.begin();
            fixture.storage.free(cleanup, segment, ColumnType.longBlob(false), allocation.value());
            fixture.manager.commit(cleanup);
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
