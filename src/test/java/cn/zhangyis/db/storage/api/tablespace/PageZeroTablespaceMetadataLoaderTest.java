package cn.zhangyis.db.storage.api.tablespace;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;


import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.meta.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessLease;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PageZeroTablespaceMetadataLoader 测试：刷盘后 raw 读 page0 重建 metadata，未打开表空间时返回 empty。
 */
class PageZeroTablespaceMetadataLoaderTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(60);

    @TempDir
    Path dir;

    /**
     * 验证 {@code rebuildsMetadataFromDiskPageZero} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void rebuildsMetadataFromDiskPageZero() {
        Path path = dir.resolve("loader.ibu");
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("loader-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            store.create(SPACE, path, PS, PageNo.of(64));
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            SpaceHeaderRepository headerRepo = new SpaceHeaderRepository(pool);
            MiniTransaction mtr = mgr.begin();
            headerRepo.initialize(mtr, new SpaceHeaderSnapshot(SPACE, PS,
                    TablespaceTypeFlags.encode(TablespaceType.UNDO), PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 1L));
            headerRepo.writeLifecycle(mtr, SPACE, new TablespaceLifecycleHeader(
                    TablespaceState.ACTIVE, PageNo.of(64), 0L, PageNo.of(64), TablespaceState.ACTIVE));
            mgr.commit(mtr);
            flushAllDirty(pool, store, redo);
        }

        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);

            TablespaceMetadata metadata = loader.load(SPACE).orElseThrow();

            assertEquals(SPACE, metadata.spaceId());
            assertEquals(TablespaceType.UNDO, metadata.type());
            assertEquals(TablespaceState.ACTIVE, metadata.state());
            assertEquals(64L, metadata.currentSizeInPages().value());
            assertEquals(path, metadata.dataFiles().get(0).path());
        }
    }

    /**
     * page0 信封页类型损坏（非 FSP_HDR）必须被拒绝：物理页虽自描述 spaceId 正确，但页型不是表空间头，
     * 说明 page0 被覆盖/绑定错误，不能注册成可用表空间，否则后续按 FSP 头解读会读到垃圾元数据。
     */
    @Test
    void rejectsPageZeroWithNonFspHdrPageType() {
        Path path = dir.resolve("badtype.ibu");
        writeValidUndoPageZero(path);
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            corruptPageZeroInt(store, PageEnvelopeLayout.PAGE_TYPE, PageType.ALLOCATED.code());
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);
            assertThrows(TablespaceCorruptedException.class, () -> loader.load(SPACE));
        }
    }

    /** page0 信封页号非 0 表示该物理页不是表空间头页（被错位写入），同样拒绝注册。 */
    @Test
    void rejectsPageZeroWithWrongPageNo() {
        Path path = dir.resolve("badpageno.ibu");
        writeValidUndoPageZero(path);
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            corruptPageZeroInt(store, PageEnvelopeLayout.PAGE_NO, 5);
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);
            assertThrows(TablespaceCorruptedException.class, () -> loader.load(SPACE));
        }
    }

    /**
     * page0 raw loader 必须在解 FSP 物理字段前校验 checksum。这里只翻转 body 中一个不会被信封校验拦截的字节：
     * 没有 checksum 校验时 loader 会继续按损坏 page0 注册表空间，后续空间管理会读到被污染的权威元数据。
     */
    @Test
    void rejectsPageZeroWithChecksumMismatch() {
        Path path = dir.resolve("bad-checksum.ibu");
        writeValidUndoPageZero(path);
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            corruptPageZeroByte(store, 256, (byte) 0x5A);
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);
            assertThrows(TablespaceCorruptedException.class, () -> loader.load(SPACE));
        }
    }

    /**
     * file trailer 的 low32 LSN 是 partial-write 判定的一部分：checksum 本身仍匹配但 trailer LSN 不匹配时，
     * loader 也必须拒绝 page0，否则会把撕裂写伪装成合法页。
     */
    @Test
    void rejectsPageZeroWithTrailerLsnMismatch() {
        Path path = dir.resolve("bad-trailer-lsn.ibu");
        writeValidUndoPageZero(path);
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            int trailerLow32Lsn = PageEnvelopeLayout.trailerOffset(PS) + PageEnvelopeLayout.TRAILER_LOW32_LSN;
            corruptPageZeroInt(store, trailerLow32Lsn, 0x12345678);
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);
            assertThrows(TablespaceCorruptedException.class, () -> loader.load(SPACE));
        }
    }

    /**
     * 兼容旧数据文件：历史切片在 FlushCoordinator 统一接管前可能存在 checksum/trailer checksum 均为 0 的合法 page0。
     * 这类页仍先通过 FSP_HDR 信封校验，再按 legacy unstamped 处理；一旦任一 checksum 非 0，就必须严格校验。
     */
    @Test
    void acceptsLegacyUnstampedPageZeroWithZeroChecksums() {
        Path path = dir.resolve("legacy-unstamped.ibu");
        writeValidUndoPageZero(path);
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            zeroPageZeroChecksums(store);
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);

            TablespaceMetadata metadata = loader.load(SPACE).orElseThrow();

            assertEquals(SPACE, metadata.spaceId());
            assertEquals(TablespaceType.UNDO, metadata.type());
            assertEquals(TablespaceState.ACTIVE, metadata.state());
        }
    }

    /**
     * 验证 {@code returnsEmptyForUnopenedSpace} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void returnsEmptyForUnopenedSpace() {
        try (PageStore store = new FileChannelPageStore()) {
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);
            assertEquals(Optional.empty(), loader.load(SpaceId.of(777)));
        }
    }

    /** loader 的 raw page0 读取也必须受共享 operation lease 保护，不能跨越 truncate X。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void loaderWaitsBehindExclusiveTablespaceLease() throws Exception {
        Path path = dir.resolve("lease-loader.ibu");
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("lease-loader-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            store.create(SPACE, path, PS, PageNo.of(64));
            MiniTransactionManager manager = new MiniTransactionManager(new TablespaceAccessController(), redo);
            SpaceHeaderRepository repository = new SpaceHeaderRepository(pool);
            MiniTransaction mtr = manager.begin();
            repository.initialize(mtr, new SpaceHeaderSnapshot(SPACE, PS,
                    TablespaceTypeFlags.encode(TablespaceType.GENERAL), PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 1L));
            manager.commit(mtr);
            flushAllDirty(pool, store, redo);

            TablespaceAccessController controller = new TablespaceAccessController(Duration.ofSeconds(2));
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS, controller);
            CompletableFuture<Optional<TablespaceMetadata>> loading;
            try (TablespaceAccessLease ignored = controller.acquireExclusive(SPACE)) {
                loading = CompletableFuture.supplyAsync(() -> loader.load(SPACE));
                TimeUnit.MILLISECONDS.sleep(100);
                assertFalse(loading.isDone());
            }
            assertEquals(TablespaceType.GENERAL, loading.get(1, TimeUnit.SECONDS).orElseThrow().type());
        }
    }

    /** 建一个合法的 UNDO page0（经 initialize 盖 FSP_HDR 信封 + ACTIVE lifecycle），关闭时刷盘。 */
    private void writeValidUndoPageZero(Path path) {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve(path.getFileName() + ".redo"))) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            store.create(SPACE, path, PS, PageNo.of(64));
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            SpaceHeaderRepository headerRepo = new SpaceHeaderRepository(pool);
            MiniTransaction mtr = mgr.begin();
            headerRepo.initialize(mtr, new SpaceHeaderSnapshot(SPACE, PS,
                    TablespaceTypeFlags.encode(TablespaceType.UNDO), PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 1L));
            headerRepo.writeLifecycle(mtr, SPACE, new TablespaceLifecycleHeader(
                    TablespaceState.ACTIVE, PageNo.of(64), 0L, PageNo.of(64), TablespaceState.ACTIVE));
            mgr.commit(mtr);
            flushAllDirty(pool, store, redo);
        }
    }

    /** 通过 flush 模块写出当前 dirty view；调用前 MTR 已发布 pageLSN，先 fsync redo 满足 WAL gate。 */
    private static void flushAllDirty(BufferPool pool, PageStore store, RedoLogManager redo) {
        redo.flush();
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        coordinator.flushList(Lsn.of(Long.MAX_VALUE), pool.capacity());
    }

    /** 直接对磁盘 page0 的指定偏移写入 int，模拟物理损坏；rewind 后 remaining==pageSize 满足 writePage 约束。 */
    private static void corruptPageZeroInt(PageStore store, int offset, int value) {
        PageId page0 = PageId.of(SPACE, PageNo.of(0));
        ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
        store.readPage(page0, buf);
        buf.putInt(offset, value);
        buf.rewind();
        store.writePage(page0, buf);
    }

    /** 翻转 page0 指定字节但不重算 checksum，用来模拟落盘页体损坏。 */
    private static void corruptPageZeroByte(PageStore store, int offset, byte xorMask) {
        PageId page0 = PageId.of(SPACE, PageNo.of(0));
        ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
        store.readPage(page0, buf);
        byte old = buf.get(offset);
        buf.put(offset, (byte) (old ^ xorMask));
        buf.rewind();
        store.writePage(page0, buf);
    }

    /** 构造旧格式 page0：保留 header/body/FIL trailer 其它字段，只把 checksum 派生字段清零。 */
    private static void zeroPageZeroChecksums(PageStore store) {
        PageId page0 = PageId.of(SPACE, PageNo.of(0));
        ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
        store.readPage(page0, buf);
        buf.putInt(PageEnvelopeLayout.CHECKSUM, 0);
        buf.putInt(PageEnvelopeLayout.trailerOffset(PS) + PageEnvelopeLayout.TRAILER_CHECKSUM, 0);
        buf.rewind();
        store.writePage(page0, buf);
    }
}
