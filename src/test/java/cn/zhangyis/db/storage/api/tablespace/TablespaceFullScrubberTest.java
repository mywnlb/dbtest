package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.dd.ddl.CreateColumnSpec;
import cn.zhangyis.db.dd.ddl.CreateIndexKeyPartSpec;
import cn.zhangyis.db.dd.ddl.CreateIndexSpec;
import cn.zhangyis.db.dd.ddl.CreateTableCommand;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLayout;
import cn.zhangyis.db.storage.fsp.extent.ExtentManagementRegionLayout;
import cn.zhangyis.db.storage.fsp.extent.XdesPageCodec;
import cn.zhangyis.db.storage.fsp.extent.XdesPageRole;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.sdi.SdiPageLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * catalog-loss 全页扫描测试：基于生产 DDL 创建的真实表空间验证成功路径与固定管理页约束。
 */
class TablespaceFullScrubberTest {

    /** 测试固定使用的 16 KiB 页大小。 */
    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path directory;

    /**
     * scanner 必须读取完整文件并绑定 table/space identity；即使攻击者重算 checksum，也不能把 page2
     * 的 INODE 信封伪装成其它已知页型。
     */
    @Test
    void scansHealthyTablespaceAndRejectsIdentityOrPageTwoTypeMismatch() throws IOException {
        Candidate candidate = createCandidate();
        TablespaceFullScrubber scrubber = new TablespaceFullScrubber();

        TablespaceFullScrubResult healthy = scrubber.scrub(request(candidate));
        assertEquals(candidate.spaceId(), healthy.spaceId());
        assertEquals(candidate.tableId(), healthy.sdi().tableId());
        assertEquals(32, healthy.fileDigest().length);
        assertTrue(healthy.pageCount() >= 4);

        TablespaceScrubException timedOut = assertThrows(TablespaceScrubException.class, () -> scrubber.scrub(
                new TablespaceFullScrubRequest(candidate.path(), PAGE_SIZE,
                        candidate.tableId(), candidate.spaceId(), Duration.ofNanos(1))));
        assertTrue(timedOut.getMessage().contains("timed out"));

        assertThrows(TablespaceScrubException.class, () -> scrubber.scrub(
                new TablespaceFullScrubRequest(candidate.path(), PAGE_SIZE,
                        candidate.tableId() + 1, candidate.spaceId(), Duration.ofSeconds(10))));

        int sdiPageNo = Math.toIntExact(SdiPageLayout.PAGE_NO);
        byte[] pageThree = readPage(candidate.path(), sdiPageNo);
        byte[] unresolvedFooter = pageThree.clone();
        unresolvedFooter[SdiPageLayout.indexBuildFooterOffset(PAGE_SIZE)] = 1;
        PageImageChecksum.stamp(unresolvedFooter, PAGE_SIZE);
        writePage(candidate.path(), sdiPageNo, unresolvedFooter);
        TablespaceScrubException footerFailure = assertThrows(
                TablespaceScrubException.class, () -> scrubber.scrub(request(candidate)));
        assertTrue(footerFailure.getMessage().contains("unresolved index DDL footer"));
        writePage(candidate.path(), sdiPageNo, pageThree);

        rewritePageTwoAsDifferentChecksummedType(candidate.path());
        TablespaceScrubException failure = assertThrows(
                TablespaceScrubException.class, () -> scrubber.scrub(request(candidate)));
        assertTrue(failure.getMessage().contains("page2 is not INODE"));
    }

    /**
     * page0 checksum 合法并不等于 XDES 语义合法：无法解码的 extent state 必须在使用 allocation
     * bitmap 前被拒绝，不能让攻击者仅重算页 checksum 就绕过空间账本校验。
     */
    @Test
    void rejectsChecksummedInvalidXdesStateOwnerAndListAddress() throws IOException {
        Candidate candidate = createCandidate();
        byte[] original = readPage(candidate.path(), 0);
        byte[] invalidState = original.clone();
        ByteBuffer.wrap(invalidState).order(ByteOrder.BIG_ENDIAN).putInt(
                ExtentDescriptorLayout.entryOffset(0) + ExtentDescriptorLayout.STATE,
                Integer.MAX_VALUE);
        PageImageChecksum.stamp(invalidState, PAGE_SIZE);
        writePage(candidate.path(), 0, invalidState);

        TablespaceScrubException stateFailure = assertThrows(
                TablespaceScrubException.class,
                () -> new TablespaceFullScrubber().scrub(request(candidate)));
        assertTrue(stateFailure.getMessage().contains("invalid XDES state"));

        byte[] invalidOwner = original.clone();
        ByteBuffer.wrap(invalidOwner).order(ByteOrder.BIG_ENDIAN).putLong(
                ExtentDescriptorLayout.entryOffset(1) + ExtentDescriptorLayout.OWNER_SEGMENT,
                99L);
        PageImageChecksum.stamp(invalidOwner, PAGE_SIZE);
        writePage(candidate.path(), 0, invalidOwner);
        TablespaceScrubException ownerFailure = assertThrows(
                TablespaceScrubException.class,
                () -> new TablespaceFullScrubber().scrub(request(candidate)));
        assertTrue(ownerFailure.getMessage().contains("invalid XDES owner"));

        byte[] invalidList = original.clone();
        ByteBuffer invalidListImage = ByteBuffer.wrap(invalidList).order(ByteOrder.BIG_ENDIAN);
        int extentOnePrevious = ExtentDescriptorLayout.entryOffset(1) + ExtentDescriptorLayout.PREV;
        invalidListImage.putLong(extentOnePrevious, 0L);
        invalidListImage.putInt(extentOnePrevious + Long.BYTES,
                ExtentDescriptorLayout.entryOffset(0) + ExtentDescriptorLayout.PREV);
        PageImageChecksum.stamp(invalidList, PAGE_SIZE);
        writePage(candidate.path(), 0, invalidList);
        TablespaceScrubException listFailure = assertThrows(
                TablespaceScrubException.class,
                () -> new TablespaceFullScrubber().scrub(request(candidate)));
        assertTrue(listFailure.getMessage().contains("invalid XDES reciprocal"));
    }

    /**
     * scanner 打开只读 channel 后若目录项属性发生变化，本轮结果不能形成 complete-scan token。
     * 故障接缝只改变 mtime，不改页内容，用于精确命中扫描前后身份复核。
     */
    @Test
    void rejectsCandidateChangedAfterReadChannelOpen() throws IOException {
        Candidate candidate = createCandidate();
        FileTime original = Files.getLastModifiedTime(candidate.path());
        AtomicBoolean hookRan = new AtomicBoolean();
        TablespaceFullScrubber scrubber = new TablespaceFullScrubber(() -> {
            try {
                Files.setLastModifiedTime(candidate.path(),
                        FileTime.fromMillis(original.toMillis() + Duration.ofMinutes(1).toMillis()));
                hookRan.set(true);
            } catch (IOException failure) {
                throw new AssertionError("cannot inject tablespace mtime change", failure);
            }
        });

        TablespaceScrubException failure = assertThrows(
                TablespaceScrubException.class, () -> scrubber.scrub(request(candidate)));

        assertTrue(hookRan.get());
        assertTrue(failure.getMessage().contains("changed during full scrub"));
    }

    /**
     * scanner 必须按运行期布局读取 group0 page5 overflow XDES，而不能继续以 page0 槽容量拒绝合法大文件；
     * header 任一范围字段损坏时即使 checksum 重算也必须 fail-closed。
     */
    @Test
    void scansStandaloneOverflowXdesAndRejectsCorruptHeader() throws IOException {
        Candidate candidate = createCandidate();
        ExtentManagementRegionLayout layout = new ExtentManagementRegionLayout(PAGE_SIZE);
        long standaloneExtent = layout.entriesPerDescriptorPage();
        int pageCount = Math.toIntExact((standaloneExtent + 1L) * PAGE_SIZE.pagesPerExtent());
        extendSparse(candidate.path(), pageCount);

        byte[] page0 = readPage(candidate.path(), 0);
        ByteBuffer pageZeroImage = ByteBuffer.wrap(page0).order(ByteOrder.BIG_ENDIAN);
        pageZeroImage.putLong(SpaceHeaderLayout.CURRENT_SIZE, pageCount);
        pageZeroImage.putLong(SpaceHeaderLayout.FREE_LIMIT, pageCount);
        int extentZeroBitmap = ExtentDescriptorLayout.entryOffset(0) + ExtentDescriptorLayout.BITMAP;
        page0[extentZeroBitmap] = (byte) (page0[extentZeroBitmap] | (1 << 5));
        PageImageChecksum.stamp(page0, PAGE_SIZE);
        writePage(candidate.path(), 0, page0);

        byte[] page5 = new byte[PAGE_SIZE.bytes()];
        ByteBuffer xdes = ByteBuffer.wrap(page5).order(ByteOrder.BIG_ENDIAN);
        xdes.putInt(PageEnvelopeLayout.SPACE_ID, candidate.spaceId().value());
        xdes.putInt(PageEnvelopeLayout.PAGE_NO, 5);
        xdes.putInt(PageEnvelopeLayout.PREV_PAGE_NO, (int) FilePageHeader.FIL_NULL);
        xdes.putInt(PageEnvelopeLayout.NEXT_PAGE_NO, (int) FilePageHeader.FIL_NULL);
        xdes.putLong(PageEnvelopeLayout.PAGE_LSN, 0L);
        xdes.putInt(PageEnvelopeLayout.PAGE_TYPE, PageType.XDES.code());
        xdes.putInt(XdesPageCodec.MAGIC_OFFSET, XdesPageCodec.MAGIC);
        xdes.putInt(XdesPageCodec.FORMAT_OFFSET, XdesPageCodec.FORMAT_VERSION);
        xdes.putInt(XdesPageCodec.ROLE_OFFSET, XdesPageRole.OVERFLOW.persistentCode());
        xdes.putLong(XdesPageCodec.GROUP_BASE_OFFSET, 0L);
        xdes.putLong(XdesPageCodec.FIRST_EXTENT_OFFSET, standaloneExtent);
        xdes.putInt(XdesPageCodec.ENTRY_COUNT_OFFSET,
                Math.toIntExact(layout.overflowEntryCount(0L)));
        PageImageChecksum.stamp(page5, PAGE_SIZE);
        writePage(candidate.path(), 5, page5);

        TablespaceFullScrubResult result = new TablespaceFullScrubber().scrub(request(candidate));
        assertEquals(pageCount, result.pageCount());

        xdes.putLong(XdesPageCodec.FIRST_EXTENT_OFFSET, standaloneExtent + 1L);
        PageImageChecksum.stamp(page5, PAGE_SIZE);
        writePage(candidate.path(), 5, page5);
        TablespaceScrubException failure = assertThrows(TablespaceScrubException.class,
                () -> new TablespaceFullScrubber().scrub(request(candidate)));
        assertTrue(failure.getMessage().contains("standalone XDES header mismatch"));
    }

    /**
     * 用公共组合根创建带 ACTIVE SDI 的真实 file-per-table，避免测试绕过 FSP/XDES/lifecycle 初始化。
     *
     * @return 已关闭引擎后仍可供离线 scanner 读取的稳定候选 identity/path
     */
    private Candidate createCandidate() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(1), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            var table = database.ddl().createTable(
                    MdlOwnerId.of(1),
                    new CreateTableCommand(
                            QualifiedTableName.of("app", "orders"),
                            PageNo.of(128),
                            List.of(new CreateColumnSpec(
                                    ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false))),
                            List.of(new CreateIndexSpec(
                                    ObjectName.of("PRIMARY"), true, true,
                                    List.of(new CreateIndexKeyPartSpec(
                                            ObjectName.of("id"), IndexOrder.ASC, 0))))),
                    Duration.ofSeconds(5));
            var binding = table.storageBinding().orElseThrow();
            return new Candidate(binding.path(), table.id().value(), binding.spaceId());
        }
    }

    /**
     * 修改 page2 页型并重新盖合法 checksum/trailer，确保断言命中语义校验而不是泛化 checksum 失败。
     *
     * @param path 已关闭 PageStore 的表空间文件
     */
    private static void rewritePageTwoAsDifferentChecksummedType(Path path) throws IOException {
        byte[] page = readPage(path, 2);
        ByteBuffer.wrap(page).order(ByteOrder.BIG_ENDIAN)
                .putInt(PageEnvelopeLayout.PAGE_TYPE, PageType.IBUF_BITMAP.code());
        PageImageChecksum.stamp(page, PAGE_SIZE);
        writePage(path, 2, page);
    }

    /** 使用显式位置完整读取单页测试镜像。 */
    private static byte[] readPage(Path path, int pageNo) throws IOException {
        byte[] page = new byte[PAGE_SIZE.bytes()];
        long position = (long) pageNo * PAGE_SIZE.bytes();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer read = ByteBuffer.wrap(page);
            while (read.hasRemaining()) {
                int count = channel.read(read, position + read.position());
                if (count < 0) {
                    throw new IOException("unexpected EOF while reading scrub test page " + pageNo);
                }
            }
        }
        return page;
    }

    /** 使用显式位置完整覆盖单页并 force，使 scanner 只观察完整测试状态。 */
    private static void writePage(Path path, int pageNo, byte[] page) throws IOException {
        long position = (long) pageNo * PAGE_SIZE.bytes();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer write = ByteBuffer.wrap(page);
            while (write.hasRemaining()) {
                channel.write(write, position + write.position());
            }
            channel.force(true);
        }
    }

    /** 只在文件尾写一个零字节形成页对齐稀疏范围，避免测试为未分配页逐页执行物理写。 */
    private static void extendSparse(Path path, int pageCount) throws IOException {
        long size = Math.multiplyExact((long) pageCount, PAGE_SIZE.bytes());
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[]{0}), size - 1L);
            channel.force(true);
        }
    }

    /** 建立与生产引擎一致的 scrub 请求。 */
    private TablespaceFullScrubRequest request(Candidate candidate) {
        return new TablespaceFullScrubRequest(candidate.path(), PAGE_SIZE,
                candidate.tableId(), candidate.spaceId(), Duration.ofSeconds(10));
    }

    /** 固定测试实例配置。 */
    private EngineConfig config() {
        return new EngineConfig(directory, PAGE_SIZE, 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    /** 生产 DDL 发布的表空间候选 identity。 */
    private record Candidate(Path path, long tableId, SpaceId spaceId) {
    }
}
