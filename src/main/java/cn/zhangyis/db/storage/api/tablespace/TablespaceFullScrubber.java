package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.SerializedDictionaryInfo;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLayout;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLocation;
import cn.zhangyis.db.storage.fsp.extent.ExtentManagementRegionLayout;
import cn.zhangyis.db.storage.fsp.extent.ExtentState;
import cn.zhangyis.db.storage.fsp.extent.XdesPageCodec;
import cn.zhangyis.db.storage.fsp.extent.XdesPageHeader;
import cn.zhangyis.db.storage.fsp.extent.XdesPageRole;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRawCodec;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.sdi.SdiPageLayout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * catalog-loss recovery 使用的 file-per-table 全页只读 scrubber。
 *
 * <p>scanner 不挂载 {@code PageStore}、不占用 SpaceId registry，也不做 doublewrite/redo 修复。page0、
 * XDES allocation bitmap、每个非零页信封/checksum、page3 SDI/footer 与扫描前后文件属性共同构成
 * 成功证明；任一不确定性都以异常交给上层 conflict 分类。</p>
 */
public final class TablespaceFullScrubber {

    /**
     * channel 已以 {@link LinkOption#NOFOLLOW_LINKS} 打开后的故障注入接缝。生产实例固定为空动作；
     * package 测试用它在扫描期间改变目录项属性，验证结束复核不会签发混合快照。
     */
    private final Runnable afterReadChannelOpen;

    /** 创建不注入扫描故障的生产 full-page scrubber。 */
    public TablespaceFullScrubber() {
        this(() -> {
        });
    }

    /**
     * 创建带 channel-open 故障接缝的 scrubber；仅供同 package 并发/TOCTOU 回归测试使用。
     *
     * @param afterReadChannelOpen channel 成功打开后、读取 page0 前执行的非空动作；不得接管或关闭 channel
     * @throws DatabaseValidationException 故障动作为空时抛出
     */
    TablespaceFullScrubber(Runnable afterReadChannelOpen) {
        if (afterReadChannelOpen == null) {
            throw new DatabaseValidationException("tablespace scrub channel-open hook must not be null");
        }
        this.afterReadChannelOpen = afterReadChannelOpen;
    }

    /**
     * 顺序扫描一个候选并生成可绑定 complete-scan token 的 fingerprint。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>NOFOLLOW 读取初始属性，校验 regular/page-aligned/至少四页，并建立单一总 deadline。</li>
     *     <li>严格校验 page0 checksum、信封、FSP identity/size/type/lifecycle/SDI root 与 XDES 覆盖范围。</li>
     *     <li>复用一个 page buffer 顺序扫描全部页：XDES 决定零页是否可接受，已分配非零页必须通过
     *     checksum、spaceId/pageNo/type；page3 额外解码 SDI 并要求 DDL footer 全零。</li>
     *     <li>扫描结束重新读取 size/mtime/fileKey；属性不变才返回全文件 SHA-256，否则拒绝 TOCTOU 候选。</li>
     * </ol>
     *
     * @param request 路径、期望 identity/page size 与共享正超时
     * @return 经过完整物理校验的不可变候选结果
     * @throws DatabaseValidationException request 为空时抛出
     * @throws TablespaceScrubException 文件损坏、身份冲突、超时、中断、属性变化或只读 IO 失败时抛出
     */
    public TablespaceFullScrubResult scrub(TablespaceFullScrubRequest request) {
        // 1. 所有 IO 使用同一 deadline；软链接和非常规文件在打开 channel 前拒绝。
        if (request == null) {
            throw new DatabaseValidationException("tablespace full scrub request must not be null");
        }
        Path path = request.path();
        long deadline = deadline(request.timeout());
        try {
            BasicFileAttributes before = attributes(path);
            int pageBytes = request.expectedPageSize().bytes();
            if (!before.isRegularFile() || before.isSymbolicLink()
                    || before.size() < (long) pageBytes * 4 || before.size() % pageBytes != 0) {
                throw new TablespaceScrubException(
                        "tablespace candidate is not a page-aligned NOFOLLOW regular file: " + path);
            }
            int pageCount = Math.toIntExact(before.size() / pageBytes);
            ByteBuffer page = ByteBuffer.allocate(pageBytes).order(ByteOrder.BIG_ENDIAN);
            MessageDigest fileDigest = sha256();
            SerializedDictionaryInfo sdi;
            SpaceHeaderPhysical physical;

            try (FileChannel channel = FileChannel.open(
                    path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                afterReadChannelOpen.run();
                checkDeadline(deadline, path, -1);
                readPage(channel, page, 0L, pageBytes, deadline, path, 0);
                byte[] page0 = page.array().clone();

                // 2. rebuild 模式不接受 legacy-zero checksum；page0 是后续 XDES/size 判断的唯一物理根。
                validatePageEnvelope(page0, request.expectedPageSize(), request.expectedSpaceId(), 0, PageType.FSP_HDR);
                physical = SpaceHeaderRawCodec.readPhysical(ByteBuffer.wrap(page0).order(ByteOrder.BIG_ENDIAN));
                validatePageZero(request, physical, pageCount, page0);

                DescriptorCatalog descriptors = loadDescriptorCatalog(channel, page, page0,
                        request, physical, pageCount, deadline);
                validateXdes(descriptors, pageCount, path);

                sdi = null;
                for (int pageNo = 0; pageNo < pageCount; pageNo++) {
                    // 3. 每页边界响应 timeout/interrupt，不在一个损坏或超大文件上无界等待。
                    checkDeadline(deadline, path, pageNo);
                    if (pageNo == 0) {
                        System.arraycopy(page0, 0, page.array(), 0, pageBytes);
                    } else {
                        readPage(channel, page, (long) pageNo * pageBytes, pageBytes,
                                deadline, path, pageNo);
                    }
                    byte[] bytes = page.array();
                    fileDigest.update(bytes);
                    boolean allocated = descriptors.isAllocated(pageNo);
                    if (isAllZero(bytes)) {
                        /*
                         * page 1 在旧版教学格式中只是尚未初始化的 change-buffer bitmap 占位页：extent0 会把它标为
                         * system allocated。当前创建路径已写入 IBUF_BITMAP 信封，只对历史文件的这个固定身份保留兼容；
                         * 其它 allocated zero page 仍表示丢失初始化内容并 fail-closed。
                         */
                        boolean reservedIbufPlaceholder = pageNo == 1 && allocated;
                        if (allocated && !reservedIbufPlaceholder) {
                            throw new TablespaceScrubException(
                                    "allocated tablespace page is all-zero: " + path + " page=" + pageNo);
                        }
                        continue;
                    }
                    if (!allocated) {
                        throw new TablespaceScrubException(
                                "unallocated tablespace page contains data: " + path + " page=" + pageNo);
                    }
                    validatePageEnvelope(bytes, request.expectedPageSize(), request.expectedSpaceId(), pageNo, null);
                    if (pageNo == 1
                            && PageType.fromCode(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                            .getInt(PageEnvelopeLayout.PAGE_TYPE)) != PageType.IBUF_BITMAP) {
                        throw new TablespaceScrubException(
                                "tablespace page1 is not IBUF_BITMAP: " + request.path());
                    }
                    if (isRepeatedBitmapPage(request.expectedPageSize(), pageNo)
                            && PageType.fromCode(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                            .getInt(PageEnvelopeLayout.PAGE_TYPE)) != PageType.IBUF_BITMAP) {
                        throw new TablespaceScrubException(
                                "tablespace repeated bitmap page has wrong type: " + request.path()
                                        + " page=" + pageNo);
                    }
                    if (descriptors.isStandaloneDescriptorPage(pageNo)
                            && PageType.fromCode(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                            .getInt(PageEnvelopeLayout.PAGE_TYPE)) != PageType.XDES) {
                        throw new TablespaceScrubException(
                                "tablespace standalone XDES page has wrong type: " + request.path()
                                        + " page=" + pageNo);
                    }
                    if (pageNo == 2
                            && PageType.fromCode(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                            .getInt(PageEnvelopeLayout.PAGE_TYPE)) != PageType.INODE) {
                        throw new TablespaceScrubException(
                                "tablespace page2 is not INODE: " + request.path());
                    }
                    if (pageNo == SdiPageLayout.PAGE_NO) {
                        sdi = decodeSdi(bytes, request);
                    }
                }
                if (sdi == null) {
                    throw new TablespaceScrubException("tablespace candidate has no valid page3 SDI: " + path);
                }
            }

            // 4. fileKey 不可用时仍比较 size/mtime；可用时再绑定底层文件对象身份。
            checkDeadline(deadline, path, pageCount);
            BasicFileAttributes after = attributes(path);
            if (!after.isRegularFile() || after.isSymbolicLink()
                    || before.size() != after.size()
                    || !before.lastModifiedTime().equals(after.lastModifiedTime())
                    || !Objects.equals(fileKey(before), fileKey(after))) {
                throw new TablespaceScrubException("tablespace candidate changed during full scrub: " + path);
            }
            return new TablespaceFullScrubResult(path, after.size(), after.lastModifiedTime().toMillis(),
                    fileKey(after), pageCount, physical.spaceId(), physical.pageSize(),
                    physical.spaceVersion(), sdi, fileDigest.digest());
        } catch (TablespaceScrubException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            if (failure instanceof TablespaceScrubException scrubFailure) {
                throw scrubFailure;
            }
            throw new TablespaceScrubException("full-page scrub failed: " + path, failure);
        }
    }

    /**
     * 建立当前 freeLimit 覆盖范围的只读 descriptor 页目录，供后续 XDES 状态、owner、FLST 与 bitmap
     * 校验使用。该步骤按统一 layout 解释 page0 与独立 XDES 镜像，不挂载 Buffer Pool，也不修复损坏账本。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 freeLimit 按 extent 对齐，并把它换算成至少包含 extent0 的已材料化范围。</li>
     *     <li>以已验证 page0 作为目录根，逐 extent 使用运行期 layout 找到实际承载 descriptor 的页面。</li>
     *     <li>补读每个已进入 freeLimit 管理区的固定 primary 与可选 overflow，即使 primary 因兼容槽仍有空 header。</li>
     *     <li>返回不可变目录；页面语义校验随后只消费该目录，不再重新执行另一套寻址公式。</li>
     * </ol>
     *
     * @param channel 当前 NOFOLLOW 只读 channel
     * @param page 可复用的整页读 buffer
     * @param page0 已通过严格 checksum、信封和 Space Header identity 校验的页镜像
     * @param request 候选 identity、页大小和路径
     * @param physical page0 物理字段快照，freeLimit 决定已材料化 XDES 范围
     * @param pageCount 文件完整页数，决定必须校验的 XDES entry 范围
     * @param deadline 全扫描共享 monotonic deadline
     * @return 已验证 descriptor 页目录，供逐页 allocation 裁决和跨页 FLST 反查复用
     * @throws TablespaceScrubException freeLimit 错位、必需管理页缺失或独立 XDES envelope/header 损坏时抛出
     */
    private static DescriptorCatalog loadDescriptorCatalog(
            FileChannel channel,
            ByteBuffer page,
            byte[] page0,
            TablespaceFullScrubRequest request,
            SpaceHeaderPhysical physical,
            int pageCount,
            long deadline) throws IOException {
        // 1. freeLimit 是 descriptor 是否已经发布的边界；物理 currentSize 的零填充尾部不能被误判为损坏 XDES。
        PageSize pageSize = request.expectedPageSize();
        int pagesPerExtent = pageSize.pagesPerExtent();
        long freeLimit = physical.freeLimitPageNo().value();
        if (freeLimit % pagesPerExtent != 0L) {
            throw new TablespaceScrubException(
                    "tablespace freeLimit is not extent aligned: " + request.path() + " freeLimit=" + freeLimit);
        }
        int extentCount = Math.toIntExact(Math.max(1L, freeLimit / pagesPerExtent));
        // 2. page0 已由调用方严格验证；逐 extent 定位会按需发现 group0 page5 和真正承载 entry 的独立页。
        ExtentManagementRegionLayout layout = new ExtentManagementRegionLayout(pageSize);
        Map<Long, byte[]> images = new HashMap<>();
        images.put(0L, page0);

        for (int extentNo = 0; extentNo < extentCount; extentNo++) {
            ExtentDescriptorLocation location = layout.locate(ExtentId.of(request.expectedSpaceId(), extentNo));
            loadStandaloneDescriptorPage(channel, page, images, location.descriptorPageId().pageNo().value(),
                    request, pageCount, deadline, layout);
        }
        // 3. 区首 fixed primary 即使 entryCount=0 也是已材料化格式证据，不能因没有 descriptor 引用而漏检。
        long lastRegion = layout.regionIndexOfExtent(extentCount - 1L);
        for (long region = 1L; region <= lastRegion; region++) {
            loadStandaloneDescriptorPage(channel, page, images, layout.primaryXdesPageNo(region).value(),
                    request, pageCount, deadline, layout);
            if (layout.requiresOverflowPage(region)) {
                loadStandaloneDescriptorPage(channel, page, images, layout.overflowXdesPageNo(region).value(),
                        request, pageCount, deadline, layout);
            }
        }
        // 4. 冻结镜像目录，后续跨页 reciprocal 校验与逐页 allocation 判断共享同一份扫描证据。
        return new DescriptorCatalog(request.expectedSpaceId(), pageSize, layout, extentCount, images);
    }

    /**
     * 读取并严格验证一张被已材料化管理区要求存在的独立 XDES 页；page0 由调用方单独提供。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>对 page0 和目录内已验证页幂等返回；其它页先证明落在当前文件完整页范围内。</li>
     *     <li>在共享 deadline 下做定点整页读取，全零页视为已发布管理证据丢失并 fail-closed。</li>
     *     <li>校验 checksum、FIL identity/type，再按统一 layout 交叉验证角色、group base 与 descriptor 范围。</li>
     *     <li>仅在全部验证成功后发布不可变页镜像；失败不改变目录，也不修复候选文件。</li>
     * </ol>
     *
     * @param channel 当前 NOFOLLOW 只读 channel
     * @param page 可复用整页 buffer
     * @param images 已验证 descriptor 页目录；本方法仅在成功后追加当前页
     * @param pageNo 由管理区 layout 计算的独立 XDES 物理页号
     * @param request 候选路径、SpaceId 与固定页大小
     * @param pageCount 候选文件完整物理页数
     * @param deadline 全 scrub 共享 monotonic deadline
     * @param layout 与运行期 repository 相同的纯寻址规则
     * @throws TablespaceScrubException 页面越界、全零、checksum、identity、type 或 XDES header 不匹配时抛出
     */
    private static void loadStandaloneDescriptorPage(
            FileChannel channel,
            ByteBuffer page,
            Map<Long, byte[]> images,
            long pageNo,
            TablespaceFullScrubRequest request,
            int pageCount,
            long deadline,
            ExtentManagementRegionLayout layout) throws IOException {
        // 1. 幂等加载避免同一页被多个 extent 重复 IO；文件范围在乘法计算 position 前验证。
        if (pageNo == 0L || images.containsKey(pageNo)) {
            return;
        }
        if (pageNo < 0L || pageNo >= pageCount || pageNo > Integer.MAX_VALUE) {
            throw new TablespaceScrubException(
                    "materialized XDES page is outside tablespace file: " + request.path() + " page=" + pageNo);
        }
        // 2. 已材料化独立页不接受文件扩展留下的全零镜像。
        readPage(channel, page, pageNo * request.expectedPageSize().bytes(),
                request.expectedPageSize().bytes(), deadline, request.path(), (int) pageNo);
        byte[] bytes = page.array().clone();
        if (isAllZero(bytes)) {
            throw new TablespaceScrubException(
                    "materialized standalone XDES page is all-zero: " + request.path() + " page=" + pageNo);
        }
        // 3. 先验证通用物理信封，再消费 XDES 专用 body，防止把业务页字节误解释成 descriptor 范围。
        validatePageEnvelope(bytes, request.expectedPageSize(), request.expectedSpaceId(),
                (int) pageNo, PageType.XDES);
        XdesPageHeader expected = expectedXdesHeader(layout, pageNo);
        ByteBuffer image = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (image.getInt(XdesPageCodec.MAGIC_OFFSET) != XdesPageCodec.MAGIC
                || image.getInt(XdesPageCodec.FORMAT_OFFSET) != XdesPageCodec.FORMAT_VERSION
                || image.getInt(XdesPageCodec.ROLE_OFFSET) != expected.role().persistentCode()
                || image.getLong(XdesPageCodec.GROUP_BASE_OFFSET) != expected.groupBasePageNo()
                || image.getLong(XdesPageCodec.FIRST_EXTENT_OFFSET) != expected.firstExtentNo()
                || image.getInt(XdesPageCodec.ENTRY_COUNT_OFFSET) != expected.entryCount()) {
            throw new TablespaceScrubException(
                    "standalone XDES header mismatch: " + request.path() + " page=" + pageNo);
        }
        // 4. 校验完成后才发布扫描镜像，异常路径保留调用方既有目录不变。
        images.put(pageNo, bytes);
    }

    /** 按与运行期 layout 相同的公式构造 raw scrub 期望 header。 */
    private static XdesPageHeader expectedXdesHeader(ExtentManagementRegionLayout layout, long pageNo) {
        long pageBytes = layout.pageSize().bytes();
        if (pageNo == 5L) {
            return new XdesPageHeader(XdesPageRole.OVERFLOW, 0L,
                    layout.entriesPerDescriptorPage(), Math.toIntExact(layout.overflowEntryCount(0L)));
        }
        if (pageNo > 0L && pageNo % pageBytes == 0L) {
            long region = pageNo / pageBytes;
            return new XdesPageHeader(XdesPageRole.PRIMARY, pageNo,
                    layout.firstStandaloneExtent(region), Math.toIntExact(layout.primaryEntryCount(region)));
        }
        if (pageNo > 5L && (pageNo - 5L) % pageBytes == 0L) {
            long region = (pageNo - 5L) / pageBytes;
            return new XdesPageHeader(XdesPageRole.OVERFLOW, region * pageBytes,
                    Math.addExact(layout.firstStandaloneExtent(region), layout.entriesPerDescriptorPage()),
                    Math.toIntExact(layout.overflowEntryCount(region)));
        }
        throw new TablespaceScrubException("invalid standalone XDES formula page: " + pageNo);
    }

    private static void validateXdes(DescriptorCatalog catalog, int pageCount, Path path) {
        // 1. ordinal 是持久格式的一部分；未知值不能退化成仅信任 bitmap 的候选。
        int pagesPerExtent = catalog.pageSize.pagesPerExtent();
        int extentCount = catalog.extentCount;
        ExtentState[] states = ExtentState.values();
        for (int extentNo = 0; extentNo < extentCount; extentNo++) {
            ExtentDescriptorLocation location = catalog.location(extentNo);
            ByteBuffer image = catalog.image(extentNo);
            int base = location.entryOffset();
            int ordinal = image.getInt(base + ExtentDescriptorLayout.STATE);
            if (ordinal < 0 || ordinal >= states.length) {
                throw new TablespaceScrubException(
                        "invalid XDES state ordinal: path=" + path + " extent=" + extentNo
                                + " ordinal=" + ordinal);
            }
            ExtentState state = states[ordinal];
            boolean managementExtent = catalog.layout.isManagementExtent(extentNo);
            if (managementExtent && state != ExtentState.FSEG_FRAG) {
                throw new TablespaceScrubException(
                        "invalid management XDES state: path=" + path + " extent=" + extentNo
                                + " state=" + state);
            }

            // 2. owner=0 是无主哨兵；每个重复管理 extent 都是 FSEG_FRAG/owner0 的系统域。
            long owner = image.getLong(base + ExtentDescriptorLayout.OWNER_SEGMENT);
            boolean segmentOwned = state == ExtentState.FSEG || state == ExtentState.FSEG_FRAG;
            if (owner < 0 || (!segmentOwned && owner != 0)
                    || (segmentOwned && !managementExtent && owner == 0)
                    || (managementExtent && owner != 0)) {
                throw new TablespaceScrubException(
                        "invalid XDES owner semantics: path=" + path + " extent=" + extentNo
                                + " state=" + state + " owner=" + owner);
            }

            // 3. 地址可跨 page0/primary/overflow，但必须由同一 layout 反解并由邻居精确反指当前 node。
            int previousExtent = validateXdesAddress(
                    catalog, image, base + ExtentDescriptorLayout.PREV, path, extentNo, "prev");
            int nextExtent = validateXdesAddress(
                    catalog, image, base + ExtentDescriptorLayout.NEXT, path, extentNo, "next");
            FileAddress currentNode = location.listNodeAddress();
            if (managementExtent && (previousExtent >= 0 || nextExtent >= 0)) {
                throw new TablespaceScrubException(
                        "management extent must not belong to an FLST: path=" + path + " extent=" + extentNo);
            }
            validateXdesReciprocalAddress(
                    catalog, previousExtent, ExtentDescriptorLayout.NEXT,
                    currentNode, state, owner, states,
                    path, extentNo, "previous.next");
            validateXdesReciprocalAddress(
                    catalog, nextExtent, ExtentDescriptorLayout.PREV,
                    currentNode, state, owner, states,
                    path, extentNo, "next.prev");

            // 4. FREE 不允许任何已分配页；FULL_FRAG 必须完整，且文件 EOF 之后不得出现幽灵分配位。
            int pagesInFile = Math.min(pagesPerExtent, pageCount - extentNo * pagesPerExtent);
            if (state == ExtentState.FULL_FRAG && pagesInFile != pagesPerExtent) {
                throw new TablespaceScrubException(
                        "FULL_FRAG XDES extent is only partially backed by file: path=" + path
                                + " extent=" + extentNo + " pages=" + pagesInFile);
            }
            for (int pageInExtent = 0; pageInExtent < pagesPerExtent; pageInExtent++) {
                int bitmapOffset = base + ExtentDescriptorLayout.BITMAP + pageInExtent / 8;
                boolean allocated = (image.get(bitmapOffset) & (1 << (pageInExtent % 8))) != 0;
                if (pageInExtent >= pagesInFile && allocated) {
                    throw new TablespaceScrubException(
                            "XDES bitmap allocates page beyond file size: path=" + path
                                    + " extent=" + extentNo + " page=" + pageInExtent);
                }
                if (state == ExtentState.FREE && allocated) {
                    throw new TablespaceScrubException(
                            "FREE XDES extent contains allocated page: path=" + path
                                    + " extent=" + extentNo + " page=" + pageInExtent);
                }
                if (state == ExtentState.FULL_FRAG && pageInExtent < pagesInFile && !allocated) {
                    throw new TablespaceScrubException(
                            "FULL_FRAG XDES extent contains free page: path=" + path
                                    + " extent=" + extentNo + " page=" + pageInExtent);
                }
            }
        }
    }

    /**
     * 校验一个原始 XDES prev/next 地址满足统一跨页 FLST node 布局。
     *
     * @param catalog 已验证 descriptor 页目录
     * @param image 当前 descriptor 页大端视图
     * @param offset FileAddress 的 12 字节起点
     * @param path 候选诊断路径
     * @param ownerExtent 持有该指针的 extent 号
     * @param field 指针字段名，仅用于诊断
     * @return NULL 地址返回 -1；非空地址返回被引用的 extent 号
     * @throws TablespaceScrubException 非 NULL 地址不属于 page0/primary/overflow、未对齐、自引用或越过 materialized 范围时抛出
     */
    private static int validateXdesAddress(
            DescriptorCatalog catalog,
            ByteBuffer image,
            int offset,
            Path path,
            int ownerExtent,
            String field) {
        long pageNo = image.getLong(offset);
        int nodeOffset = image.getInt(offset + Long.BYTES);
        if (pageNo == 0 && nodeOffset == 0) {
            return -1;
        }
        int referencedExtent;
        try {
            referencedExtent = Math.toIntExact(catalog.layout.extentIdOfNode(catalog.spaceId,
                    FileAddress.of(PageNo.of(pageNo), nodeOffset)).extentNo());
        } catch (RuntimeException invalid) {
            throw new TablespaceScrubException(
                    "invalid XDES " + field + " address: path=" + path + " extent=" + ownerExtent
                            + " page=" + pageNo + " offset=" + nodeOffset, invalid);
        }
        if (referencedExtent >= catalog.extentCount || referencedExtent == ownerExtent) {
            throw new TablespaceScrubException(
                    "invalid XDES " + field + " address: path=" + path + " extent=" + ownerExtent
                            + " page=" + pageNo + " offset=" + nodeOffset);
        }
        return referencedExtent;
    }

    /**
     * 校验 XDES 双向链节点的反向地址精确指回当前 entry，拒绝只有单边看似对齐的损坏指针。
     *
     * @param catalog 已验证 descriptor 页目录
     * @param referencedExtent 前向字段引用的 extent；-1 表示 NULL，无需反向检查
     * @param reciprocalField 被引用 entry 中应当反指当前节点的 PREV 或 NEXT 字段偏移
     * @param expectedNode 当前 entry 的跨页 PREV 节点地址
     * @param expectedState 当前 entry 的 XDES 状态；同一物理链中的邻居必须一致
     * @param expectedOwner 当前 entry 的 segment owner；同一 segment 链中的邻居必须一致
     * @param states 持久 ordinal 到领域状态的固定映射
     * @param path 候选诊断路径
     * @param ownerExtent 当前 entry 号
     * @param relation 反向关系诊断名
     * @throws TablespaceScrubException 反向指针不是 page0 当前节点地址时抛出
     */
    private static void validateXdesReciprocalAddress(
            DescriptorCatalog catalog,
            int referencedExtent,
            int reciprocalField,
            FileAddress expectedNode,
            ExtentState expectedState,
            long expectedOwner,
            ExtentState[] states,
            Path path,
            int ownerExtent,
            String relation) {
        if (referencedExtent < 0) {
            return;
        }
        ExtentDescriptorLocation targetLocation = catalog.location(referencedExtent);
        ByteBuffer targetImage = catalog.image(referencedExtent);
        int targetBase = targetLocation.entryOffset();
        int reciprocal = targetBase + reciprocalField;
        long reciprocalPage = targetImage.getLong(reciprocal);
        int reciprocalOffset = targetImage.getInt(reciprocal + Long.BYTES);
        int targetOrdinal = targetImage.getInt(targetBase + ExtentDescriptorLayout.STATE);
        long targetOwner = targetImage.getLong(targetBase + ExtentDescriptorLayout.OWNER_SEGMENT);
        if (reciprocalPage != expectedNode.pageNo().value()
                || reciprocalOffset != expectedNode.offset()) {
            throw new TablespaceScrubException(
                    "invalid XDES reciprocal " + relation + " address: path=" + path
                            + " extent=" + ownerExtent + " referencedExtent=" + referencedExtent);
        }
        if (targetOrdinal < 0 || targetOrdinal >= states.length
                || states[targetOrdinal] != expectedState || targetOwner != expectedOwner) {
            throw new TablespaceScrubException(
                    "XDES list crosses state or owner boundary: path=" + path
                            + " extent=" + ownerExtent + " referencedExtent=" + referencedExtent);
        }
    }

    /** 校验 page0 自描述、文件长度、GENERAL lifecycle 与固定 SDI root。 */
    private static void validatePageZero(TablespaceFullScrubRequest request, SpaceHeaderPhysical physical,
                                         int pageCount, byte[] page0) {
        if (!physical.spaceId().equals(request.expectedSpaceId())
                || !physical.pageSize().equals(request.expectedPageSize())
                || physical.currentSizeInPages().value() != pageCount
                || physical.freeLimitPageNo().value() > pageCount
                || physical.spaceVersion() <= 0) {
            throw new TablespaceScrubException("tablespace page0 identity/size is inconsistent: " + request.path());
        }
        TablespaceType type = TablespaceTypeFlags.decode(physical.spaceFlags());
        if (type != TablespaceType.GENERAL && type != TablespaceType.FILE_PER_TABLE) {
            throw new TablespaceScrubException("catalog recovery candidate is not a user tablespace: " + type);
        }
        TablespaceLifecycleHeader lifecycle = TablespaceLifecycleRawCodec
                .read(ByteBuffer.wrap(page0).order(ByteOrder.BIG_ENDIAN))
                .orElseThrow(() -> new TablespaceScrubException(
                        "catalog recovery candidate lacks lifecycle marker: " + request.path()));
        if (lifecycle.state() != TablespaceState.NORMAL
                || lifecycle.initialSizeInPages().value() > pageCount) {
            throw new TablespaceScrubException(
                    "catalog recovery candidate is not stable NORMAL: " + lifecycle.state());
        }
        long root = ByteBuffer.wrap(page0).order(ByteOrder.BIG_ENDIAN).getLong(SpaceHeaderLayout.SDI_ROOT);
        if (root != SdiPageLayout.PAGE_NO) {
            throw new TablespaceScrubException("catalog recovery candidate has unsupported SDI root: " + root);
        }
    }

    /** 校验严格 checksum 和物理信封；expectedType 为空时仍拒绝未知 page type。 */
    private static void validatePageEnvelope(byte[] bytes, PageSize pageSize, SpaceId spaceId,
                                             int pageNo, PageType expectedType) {
        if (!PageImageChecksum.verify(bytes, pageSize)) {
            throw new TablespaceScrubException("tablespace page checksum/trailer mismatch: page=" + pageNo);
        }
        ByteBuffer page = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (page.getInt(PageEnvelopeLayout.SPACE_ID) != spaceId.value()
                || page.getInt(PageEnvelopeLayout.PAGE_NO) != pageNo) {
            throw new TablespaceScrubException("tablespace page envelope identity mismatch: page=" + pageNo);
        }
        PageType actual = PageType.fromCode(page.getInt(PageEnvelopeLayout.PAGE_TYPE));
        if (expectedType != null && actual != expectedType) {
            throw new TablespaceScrubException(
                    "tablespace page type mismatch: page=" + pageNo + " actual=" + actual);
        }
    }

    /** 解码 page3 opaque SDI、payload CRC 与全零 index-DDL footer。 */
    private static SerializedDictionaryInfo decodeSdi(byte[] bytes, TablespaceFullScrubRequest request) {
        ByteBuffer page = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (PageType.fromCode(page.getInt(PageEnvelopeLayout.PAGE_TYPE)) != PageType.SDI
                || page.getInt(SdiPageLayout.MAGIC_OFFSET) != SdiPageLayout.MAGIC
                || page.getInt(SdiPageLayout.FORMAT_OFFSET) != SdiPageLayout.FORMAT_VERSION) {
            throw new TablespaceScrubException("tablespace page3 SDI envelope/format is invalid: " + request.path());
        }
        long tableId = page.getLong(SdiPageLayout.TABLE_ID_OFFSET);
        long version = page.getLong(SdiPageLayout.DICTIONARY_VERSION_OFFSET);
        int length = page.getInt(SdiPageLayout.PAYLOAD_LENGTH_OFFSET);
        int expectedCrc = page.getInt(SdiPageLayout.PAYLOAD_CRC32C_OFFSET);
        if (tableId != request.expectedTableId() || version <= 0 || length <= 0
                || length > SdiPageLayout.payloadCapacity(request.expectedPageSize())) {
            throw new TablespaceScrubException(
                    "tablespace page3 SDI identity/length is invalid: " + request.path());
        }
        byte[] payload = Arrays.copyOfRange(bytes, SdiPageLayout.PAYLOAD_OFFSET,
                SdiPageLayout.PAYLOAD_OFFSET + length);
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);
        if ((int) crc.getValue() != expectedCrc) {
            throw new TablespaceScrubException("tablespace page3 SDI payload CRC32C mismatch: " + request.path());
        }
        int footer = SdiPageLayout.indexBuildFooterOffset(request.expectedPageSize());
        for (int offset = footer; offset < footer + SdiPageLayout.INDEX_BUILD_FOOTER_BYTES; offset++) {
            if (bytes[offset] != 0) {
                throw new TablespaceScrubException(
                        "tablespace page3 contains unresolved index DDL footer: " + request.path());
            }
        }
        return new SerializedDictionaryInfo(tableId, version, payload);
    }

    /** 判断页号是否是 {@code 1 + k*pageSize.bytes()} 的重复 IBUF_BITMAP 固定位置。 */
    private static boolean isRepeatedBitmapPage(PageSize pageSize, int pageNo) {
        return pageNo >= 1 && (pageNo - 1L) % pageSize.bytes() == 0L;
    }

    /**
     * scrub 期间只读持有的 XDES 页目录。它只缓存少量管理页镜像，不缓存业务页；所有 extent/node 反查
     * 都复用运行期 {@link ExtentManagementRegionLayout}，避免离线工具复制另一套地址公式。
     */
    private static final class DescriptorCatalog {

        /** 候选表空间 identity，用于把持久 FileAddress 还原为 ExtentId。 */
        private final SpaceId spaceId;
        /** bitmap 位宽与 extent 换算使用的实例页大小。 */
        private final PageSize pageSize;
        /** page0/primary/overflow 唯一布局公式。 */
        private final ExtentManagementRegionLayout layout;
        /** freeLimit 已材料化的 extent 数；文件尾的零填充 extent 不要求 descriptor 页。 */
        private final int extentCount;
        /** pageNo 到已通过 checksum/envelope/header 校验的原始页镜像。 */
        private final Map<Long, byte[]> images;

        private DescriptorCatalog(SpaceId spaceId, PageSize pageSize,
                                  ExtentManagementRegionLayout layout, int extentCount,
                                  Map<Long, byte[]> images) {
            this.spaceId = spaceId;
            this.pageSize = pageSize;
            this.layout = layout;
            this.extentCount = extentCount;
            this.images = Map.copyOf(images);
        }

        /** 返回 extent 的稳定物理槽位；范围在调用点由 materialized extentCount 保证。 */
        private ExtentDescriptorLocation location(long extentNo) {
            return layout.locate(ExtentId.of(spaceId, extentNo));
        }

        /** 返回 extent 所在 descriptor 页的大端只读包装；缺页表示 catalog 构建逻辑或持久格式损坏。 */
        private ByteBuffer image(long extentNo) {
            ExtentDescriptorLocation location = location(extentNo);
            byte[] bytes = images.get(location.descriptorPageId().pageNo().value());
            if (bytes == null) {
                throw new TablespaceScrubException(
                        "validated XDES image is missing for extent " + extentNo);
            }
            return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        }

        /** 依据已材料化 descriptor bitmap 裁决页；freeLimit 之外的物理零填充一律视为未分配。 */
        private boolean isAllocated(int pageNo) {
            int pagesPerExtent = pageSize.pagesPerExtent();
            long extentNo = pageNo / (long) pagesPerExtent;
            if (extentNo >= extentCount) {
                return false;
            }
            ExtentDescriptorLocation location = location(extentNo);
            ByteBuffer image = image(extentNo);
            int pageInExtent = pageNo % pagesPerExtent;
            int offset = location.entryOffset() + ExtentDescriptorLayout.BITMAP + pageInExtent / 8;
            return (image.get(offset) & (1 << (pageInExtent % 8))) != 0;
        }

        /** page0 不是独立 XDES；其它已加载 key 包含空 primary 和真正承载 entries 的管理页。 */
        private boolean isStandaloneDescriptorPage(int pageNo) {
            return pageNo != 0 && images.containsKey((long) pageNo);
        }
    }

    /** 判断整页是否仍为文件扩展留下的零填充未分配页。 */
    private static boolean isAllZero(byte[] bytes) {
        for (byte value : bytes) {
            if (value != 0) {
                return false;
            }
        }
        return true;
    }

    /** 使用显式位置完整读取一个物理页。 */
    private static void readPage(FileChannel channel, ByteBuffer page, long position, int pageBytes,
                                 long deadline, Path path, int pageNo)
            throws IOException {
        page.clear();
        int read = 0;
        while (page.hasRemaining()) {
            checkDeadline(deadline, path, pageNo);
            int count = channel.read(page, position + read);
            if (count < 0) {
                throw new IOException("unexpected EOF while reading tablespace page");
            }
            if (count == 0) {
                Thread.onSpinWait();
                continue;
            }
            read += count;
        }
        page.position(0);
        page.limit(pageBytes);
    }

    /** NOFOLLOW 获取文件属性。 */
    private static BasicFileAttributes attributes(Path path) throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    }

    /** 把可选 fileKey 转为稳定诊断/token 字段。 */
    private static String fileKey(BasicFileAttributes attributes) {
        return attributes.fileKey() == null ? "" : attributes.fileKey().toString();
    }

    /** 建立单一 monotonic deadline，溢出时饱和为 Long.MAX_VALUE。 */
    private static long deadline(Duration timeout) {
        try {
            long nanos = timeout.toNanos();
            long now = System.nanoTime();
            long deadline = now + nanos;
            return deadline < 0 && now > 0 ? Long.MAX_VALUE : deadline;
        } catch (ArithmeticException failure) {
            return Long.MAX_VALUE;
        }
    }

    /** 在页边界检查中断和 deadline；中断状态保持给上层观察。 */
    private static void checkDeadline(long deadline, Path path, int pageNo) {
        if (Thread.currentThread().isInterrupted()) {
            throw new TablespaceScrubException(
                    "tablespace full scrub interrupted: " + path + " page=" + pageNo);
        }
        if (deadline != Long.MAX_VALUE && System.nanoTime() - deadline >= 0) {
            throw new TablespaceScrubException(
                    "tablespace full scrub timed out: " + path + " page=" + pageNo);
        }
    }

    /** 获取标准 SHA-256 实例。 */
    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new TablespaceScrubException("SHA-256 is unavailable", failure);
        }
    }
}
