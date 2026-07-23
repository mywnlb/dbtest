package cn.zhangyis.db.storage.api.tablespace;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;


import cn.zhangyis.db.domain.ExtentId;
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
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLayout;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLocation;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.extent.ExtentManagementRegionLayout;
import cn.zhangyis.db.storage.fsp.extent.ExtentState;
import cn.zhangyis.db.storage.fsp.extent.XdesPageCodec;
import cn.zhangyis.db.storage.fsp.extent.XdesPageRole;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.flst.FlstBaseLayout;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
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
    private static final PageSize SMALL_PS = PageSize.ofBytes(4 * 1024);
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

    /**
     * 已发布 freeLimit 跨过重复管理区时，普通打开必须把缺失的 primary XDES/bitmap 当成损坏，而恢复打开仍须
     * 允许注册 page0 身份，以便后续 PAGE_INIT redo 补齐这些页。若普通路径只检查 page0，这个文件会被错误发布，
     * allocator 随后可能从旧 FSP_FREE 摘下管理 extent 并在无 content-undo 的 MTR 中留下半修改链表。
     */
    @Test
    void rejectsCrossedManagementRegionMissingFixedPagesButRecoveryLoadRemainsAvailable() {
        Path path = dir.resolve("missing-crossed-management-region.ibu");
        writeCrossedRegionTablespace(path, false, false);
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, SMALL_PS);
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, SMALL_PS);

            assertThrows(TablespaceCorruptedException.class, () -> loader.load(SPACE));
            assertEquals(SPACE, loader.loadForRecovery(SPACE).orElseThrow().spaceId());
        }
    }

    /** 完整的 region1 fixed pages、canonical descriptor 与空全局链必须通过普通打开，防止校验器只会拒绝。 */
    @Test
    void acceptsCompleteCrossedManagementRegion() {
        Path path = dir.resolve("complete-crossed-management-region.ibu");
        writeCrossedRegionTablespace(path, true, false);
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, SMALL_PS);

            TablespaceMetadata metadata = new PageZeroTablespaceMetadataLoader(store, SMALL_PS)
                    .load(SPACE).orElseThrow();

            assertEquals(SPACE, metadata.spaceId());
            assertEquals(TablespaceType.GENERAL, metadata.type());
        }
    }

    /**
     * 即使重复管理页、XDES header 与管理 descriptor 都合法，page0 的任一全局 extent-list base 也不得把该
     * 管理 extent 当成单节点链成员。descriptor 自身的 prev/next 都是 NULL，只有显式核对 base 的 first/last
     * 才能发现这种旧格式冲突；普通打开必须在首次分配之前拒绝它。
     */
    @Test
    void rejectsManagementExtentReferencedBySingleNodeFreeListBase() {
        Path path = dir.resolve("management-extent-in-free-list.ibu");
        writeCrossedRegionTablespace(path, true, true);
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, SMALL_PS);
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, SMALL_PS);

            assertThrows(TablespaceCorruptedException.class, () -> loader.load(SPACE));
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

    /**
     * 构造 freeLimit 已完整消费 region1 管理 extent 的 4KiB 表空间。可选分支会写入合法 fixed pages 与
     * canonical descriptor，再故意仅让 FSP_FREE base 指向该管理节点，用于区分 header/descriptor 校验和
     * 全局链表 membership 校验。
     *
     * @param path 测试目录内尚不存在的数据文件
     * @param formatManagementRegion {@code true} 时写入合法 region1 fixed pages 与 canonical descriptor
     * @param linkManagementExtentToFreeList {@code true} 时额外创建“固定页合法但 FSP_FREE 错链”的镜像
     */
    private void writeCrossedRegionTablespace(Path path, boolean formatManagementRegion,
                                              boolean linkManagementExtentToFreeList) {
        ExtentManagementRegionLayout layout = new ExtentManagementRegionLayout(SMALL_PS);
        long managementExtentNo = layout.extentsPerManagementRegion();
        long freeLimit = Math.multiplyExact(managementExtentNo + 1L, SMALL_PS.pagesPerExtent());
        PageNo size = PageNo.of(freeLimit);
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, SMALL_PS, 32);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(
                     dir.resolve(path.getFileName() + ".crossed.redo"))) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            store.create(SPACE, path, SMALL_PS, size);
            MiniTransactionManager manager = new MiniTransactionManager(new TablespaceAccessController(), redo);
            MiniTransaction mtr = manager.begin();
            new SpaceHeaderRepository(pool).initialize(mtr, new SpaceHeaderSnapshot(
                    SPACE, SMALL_PS, TablespaceTypeFlags.encode(TablespaceType.GENERAL), size, size, 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 1L));
            new ExtentDescriptorRepository(pool, SMALL_PS).reserveSystemExtent(mtr, SPACE);
            manager.commit(mtr);
            flushAllDirty(pool, store, redo, SMALL_PS);

            if (formatManagementRegion) {
                writeRegionOneFixedPages(store, layout);
                writeCanonicalManagementDescriptor(store, layout, managementExtentNo,
                        linkManagementExtentToFreeList);
            }
            store.force(SPACE);
        }
    }

    /** 写入 region1 的合法 primary XDES 与重复 IBUF_BITMAP 页；4KiB 布局不需要 overflow 页。 */
    private static void writeRegionOneFixedPages(PageStore store, ExtentManagementRegionLayout layout) {
        long region = 1L;
        long primaryPageNo = layout.primaryXdesPageNo(region).value();
        byte[] primary = new byte[SMALL_PS.bytes()];
        ByteBuffer primaryView = ByteBuffer.wrap(primary);
        writeEnvelope(primaryView, primaryPageNo, PageType.XDES);
        primaryView.putInt(XdesPageCodec.MAGIC_OFFSET, XdesPageCodec.MAGIC);
        primaryView.putInt(XdesPageCodec.FORMAT_OFFSET, XdesPageCodec.FORMAT_VERSION);
        primaryView.putInt(XdesPageCodec.ROLE_OFFSET, XdesPageRole.PRIMARY.persistentCode());
        primaryView.putLong(XdesPageCodec.GROUP_BASE_OFFSET, primaryPageNo);
        primaryView.putLong(XdesPageCodec.FIRST_EXTENT_OFFSET, layout.firstStandaloneExtent(region));
        primaryView.putInt(XdesPageCodec.ENTRY_COUNT_OFFSET,
                Math.toIntExact(layout.primaryEntryCount(region)));
        PageImageChecksum.stamp(primary, SMALL_PS);
        store.writePage(PageId.of(SPACE, PageNo.of(primaryPageNo)), ByteBuffer.wrap(primary));

        long bitmapPageNo = layout.bitmapPageNo(region).value();
        byte[] bitmap = new byte[SMALL_PS.bytes()];
        writeEnvelope(ByteBuffer.wrap(bitmap), bitmapPageNo, PageType.IBUF_BITMAP);
        PageImageChecksum.stamp(bitmap, SMALL_PS);
        store.writePage(PageId.of(SPACE, PageNo.of(bitmapPageNo)), ByteBuffer.wrap(bitmap));
    }

    /** 把 region1 管理 descriptor 写成 canonical image，并按夹具开关选择是否伪造 FSP_FREE 单节点端点。 */
    private static void writeCanonicalManagementDescriptor(
            PageStore store, ExtentManagementRegionLayout layout, long managementExtentNo,
            boolean linkManagementExtentToFreeList) {
        PageId page0Id = PageId.of(SPACE, PageNo.of(0));
        ByteBuffer page0 = ByteBuffer.allocate(SMALL_PS.bytes());
        store.readPage(page0Id, page0);
        ExtentDescriptorLocation location = layout.locate(ExtentId.of(SPACE, managementExtentNo));
        int entry = location.entryOffset();
        page0.putInt(entry + ExtentDescriptorLayout.STATE, ExtentState.FSEG_FRAG.ordinal());
        page0.putLong(entry + ExtentDescriptorLayout.OWNER_SEGMENT, 0L);
        page0.put(entry + ExtentDescriptorLayout.BITMAP, (byte) 0b0000_0011);

        if (linkManagementExtentToFreeList) {
            int base = SpaceHeaderLayout.FREE_EXTENT_LIST_BASE;
            page0.putLong(base + FlstBaseLayout.LEN, 1L);
            putAddress(page0, base + FlstBaseLayout.FIRST,
                    location.listNodeAddress().pageNo().value(), location.listNodeAddress().offset());
            putAddress(page0, base + FlstBaseLayout.LAST,
                    location.listNodeAddress().pageNo().value(), location.listNodeAddress().offset());
        }

        byte[] image = page0.array();
        PageImageChecksum.stamp(image, SMALL_PS);
        store.writePage(page0Id, ByteBuffer.wrap(image));
    }

    /** 写入本测试所需的最小 FIL envelope；checksum/trailer 在调用方完成 body 后统一盖戳。 */
    private static void writeEnvelope(ByteBuffer page, long pageNo, PageType pageType) {
        page.putInt(PageEnvelopeLayout.SPACE_ID, SPACE.value());
        page.putInt(PageEnvelopeLayout.PAGE_NO, Math.toIntExact(pageNo));
        page.putInt(PageEnvelopeLayout.PREV_PAGE_NO, -1);
        page.putInt(PageEnvelopeLayout.NEXT_PAGE_NO, -1);
        page.putLong(PageEnvelopeLayout.PAGE_LSN, 0L);
        page.putInt(PageEnvelopeLayout.PAGE_TYPE, pageType.code());
    }

    /** 按持久 FileAddress 的 long pageNo + int offset 编码写入非空地址。 */
    private static void putAddress(ByteBuffer page, int offset, long pageNo, int pageOffset) {
        page.putLong(offset, pageNo);
        page.putInt(offset + Long.BYTES, pageOffset);
    }

    /** 通过 flush 模块写出当前 dirty view；调用前 MTR 已发布 pageLSN，先 fsync redo 满足 WAL gate。 */
    private static void flushAllDirty(BufferPool pool, PageStore store, RedoLogManager redo) {
        flushAllDirty(pool, store, redo, PS);
    }

    /** 允许 4KiB 跨区夹具复用同一 WAL 后刷盘路径。 */
    private static void flushAllDirty(BufferPool pool, PageStore store, RedoLogManager redo, PageSize pageSize) {
        redo.flush();
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, pageSize,
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
