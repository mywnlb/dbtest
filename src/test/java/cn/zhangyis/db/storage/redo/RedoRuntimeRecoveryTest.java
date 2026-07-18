package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R1 redo runtime：验证 redo 批次可落盘、可等待 durable，并可由 recovery reader/dispatcher 幂等回放 PAGE_INIT/PAGE_BYTES。
 */
class RedoRuntimeRecoveryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId P = PageId.of(SPACE, PageNo.of(3));
    private static final int PAYLOAD_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 16;

    @TempDir
    Path dir;

    /**
     * 验证 {@code flushPersistsBatchAndWaitsForDurability} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void flushPersistsBatchAndWaitsForDurability() {
        Path redoPath = dir.resolve("redo.log");
        LogRange range;

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager mgr = RedoLogManager.durable(repo);
            range = mgr.append(List.of(
                    new PageInitRecord(P, PageType.INDEX),
                    new PageBytesRecord(P, PAYLOAD_OFFSET, new byte[]{1, 2, 3})));

            assertEquals(Lsn.of(0), mgr.flushedToDiskLsn(), "append only reserves LSN before flush");
            assertEquals(range.end(), mgr.flush());
            assertTrue(mgr.waitFlushed(range.end(), Duration.ofMillis(100)), "target LSN becomes durable");
        }

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoRecoveryReader reader = new RedoRecoveryReader(repo);
            List<RedoLogBatch> batches = reader.readBatches();
            assertEquals(1, batches.size());
            assertEquals(range, batches.get(0).range());
            assertEquals(range.end(), reader.recoveredToLsn());
            assertTrue(batches.get(0).records().get(0) instanceof PageInitRecord);
            assertTrue(batches.get(0).records().get(1) instanceof PageBytesRecord);
        }
    }

    /**
     * 验证 {@code waitFlushedTimesOutWhenNoFlusherAdvances} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void waitFlushedTimesOutWhenNoFlusherAdvances() {
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            RedoLogManager mgr = RedoLogManager.durable(repo);
            LogRange range = mgr.append(List.of(new PageInitRecord(P, PageType.INDEX)));

            assertFalse(mgr.waitFlushed(range.end(), Duration.ofMillis(10)), "unflushed redo is not durable");
            assertEquals(Lsn.of(0), mgr.flushedToDiskLsn());
        }
    }

    /**
     * 验证 {@code recoveryAppliesPageInitAndBytesThenStampsBatchEndLsn} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void recoveryAppliesPageInitAndBytesThenStampsBatchEndLsn() {
        byte[] payload = new byte[]{9, 8, 7, 6};
        LogRange range;
        Path redoPath = dir.resolve("redo.log");

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager mgr = RedoLogManager.durable(repo);
            range = mgr.append(List.of(
                    new PageInitRecord(P, PageType.INDEX),
                    new PageBytesRecord(P, PAYLOAD_OFFSET, payload)));
            mgr.flush();
        }

        try (PageStore store = createStore("s.ibd");
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoRecoveryReader reader = new RedoRecoveryReader(repo);
            RedoApplyDispatcher.pageDispatcher().applyAll(reader.readBatches(), new RedoApplyContext(store, PS));

            byte[] page = readPage(store, P);
            ByteBuffer view = ByteBuffer.wrap(page);
            assertEquals(PageType.INDEX.code(), view.getInt(PageEnvelopeLayout.PAGE_TYPE));
            assertEquals(range.end().value(), view.getLong(PageEnvelopeLayout.PAGE_LSN));
            assertArrayEquals(payload, slice(page, PAYLOAD_OFFSET, payload.length));
        }
    }

    /** LOB 不新增专用 redo record；稳定 PageType code 经既有 PAGE_INIT/PAGE_BYTES dispatcher 原样恢复。 */
    @Test
    void genericRecoveryReplaysBlobPageTypeAndBody() {
        byte[] payload = new byte[]{0x4c, 0x4f, 0x42, 0x31};
        Path redoPath = dir.resolve("lob-redo.log");
        LogRange range;
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager manager = RedoLogManager.durable(repo);
            range = manager.append(List.of(
                    new PageInitRecord(P, PageType.BLOB),
                    new PageBytesRecord(P, PAYLOAD_OFFSET, payload)));
            manager.flush();
        }
        try (PageStore store = createStore("lob.ibd");
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoApplyDispatcher.pageDispatcher().applyAll(
                    new RedoRecoveryReader(repo).readBatches(), new RedoApplyContext(store, PS));
            byte[] page = readPage(store, P);
            ByteBuffer view = ByteBuffer.wrap(page);
            assertEquals(PageType.BLOB.code(), view.getInt(PageEnvelopeLayout.PAGE_TYPE));
            assertEquals(range.end().value(), view.getLong(PageEnvelopeLayout.PAGE_LSN));
            assertArrayEquals(payload, slice(page, PAYLOAD_OFFSET, payload.length));
        }
    }

    /**
     * 验证 {@code recoverySkipsPageWhosePageLsnAlreadyCoversBatch} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void recoverySkipsPageWhosePageLsnAlreadyCoversBatch() {
        byte[] oldPayload = new byte[]{4, 4, 4};
        byte[] redoPayload = new byte[]{7, 7, 7};
        LogRange range;

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            RedoLogManager mgr = RedoLogManager.durable(repo);
            range = mgr.append(List.of(
                    new PageInitRecord(P, PageType.INDEX),
                    new PageBytesRecord(P, PAYLOAD_OFFSET, redoPayload)));
            mgr.flush();
        }

        try (PageStore store = createStore("s.ibd")) {
            byte[] existing = new byte[PS.bytes()];
            ByteBuffer view = ByteBuffer.wrap(existing);
            writeHeader(view, P, PageType.INDEX, range.end());
            view.position(PAYLOAD_OFFSET);
            view.put(oldPayload);
            store.writePage(P, ByteBuffer.wrap(existing));

            RedoLogBatch batch = new RedoLogBatch(range, List.of(
                    new PageInitRecord(P, PageType.INDEX),
                    new PageBytesRecord(P, PAYLOAD_OFFSET, redoPayload)));
            RedoApplyDispatcher.pageDispatcher().apply(batch, new RedoApplyContext(store, PS));

            byte[] page = readPage(store, P);
            assertEquals(range.end().value(), ByteBuffer.wrap(page).getLong(PageEnvelopeLayout.PAGE_LSN));
            assertArrayEquals(oldPayload, slice(page, PAYLOAD_OFFSET, oldPayload.length));
        }
    }

    /**
     * 验证 {@code recoveryReaderStopsAtIncompleteTail} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void recoveryReaderStopsAtIncompleteTail() throws Exception {
        Path redoPath = dir.resolve("redo.log");
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager mgr = RedoLogManager.durable(repo);
            mgr.append(List.of(new PageInitRecord(P, PageType.INDEX)));
            mgr.flush();
        }
        Files.write(redoPath, new byte[]{0x52, 0x4c}, java.nio.file.StandardOpenOption.APPEND);

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoRecoveryReader reader = new RedoRecoveryReader(repo);
            List<RedoLogBatch> batches = reader.readBatches();
            assertEquals(1, batches.size(), "complete batch before torn tail remains readable");
            assertEquals(batches.get(0).range().end(), reader.recoveredToLsn());
        }
    }

    private PageStore createStore(String name) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve(name), PS, PageNo.of(8));
        return store;
    }

    private static byte[] readPage(PageStore store, PageId pageId) {
        byte[] page = new byte[PS.bytes()];
        store.readPage(pageId, ByteBuffer.wrap(page));
        return page;
    }

    private static byte[] slice(byte[] bytes, int offset, int length) {
        byte[] out = new byte[length];
        System.arraycopy(bytes, offset, out, 0, length);
        return out;
    }

    private static void writeHeader(ByteBuffer page, PageId pageId, PageType type, Lsn lsn) {
        page.putInt(PageEnvelopeLayout.SPACE_ID, pageId.spaceId().value());
        page.putInt(PageEnvelopeLayout.PAGE_NO, (int) pageId.pageNo().value());
        page.putInt(PageEnvelopeLayout.PREV_PAGE_NO, (int) FilePageHeader.FIL_NULL);
        page.putInt(PageEnvelopeLayout.NEXT_PAGE_NO, (int) FilePageHeader.FIL_NULL);
        page.putLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
        page.putInt(PageEnvelopeLayout.PAGE_TYPE, type.code());
    }
}
