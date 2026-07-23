package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLayout;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLocation;
import cn.zhangyis.db.storage.fsp.extent.ExtentManagementRegionLayout;
import cn.zhangyis.db.storage.fsp.extent.ExtentState;
import cn.zhangyis.db.storage.fsp.extent.XdesPageCodec;
import cn.zhangyis.db.storage.fsp.extent.XdesPageHeader;
import cn.zhangyis.db.storage.fsp.extent.XdesPageRole;
import cn.zhangyis.db.storage.fsp.flst.FlstBaseLayout;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.page.PageType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 普通表空间打开阶段的已材料化 XDES 管理目录校验器。
 *
 * <p>该类只通过 {@link PageStore} raw 读固定管理页，不挂载 Buffer Pool、不产生 redo，也不尝试在线迁移旧
 * FSP_FREE。page0 的 {@code freeLimit} 是发布边界：一旦越过某个管理 extent，该区 primary XDES、重复
 * IBUF_BITMAP、可选 overflow 和 canonical 管理 descriptor 必须同时存在，且管理节点不能出现在 page0 的
 * 三个全局 extent-list base 中。任何不一致在 registry 发布普通句柄前 fail-closed。</p>
 *
 * <p>恢复打开不调用本类，因为崩溃点可能已经持久化 page0/freeLimit redo、但管理页 PAGE_INIT 尚未写回数据
 * 文件；redo/doublewrite 完成后，registry 的 recovery-only 句柄会在首次普通 require 时重新经过本校验。</p>
 */
final class MaterializedXdesAdmissionValidator {

    /** 已打开文件的物理整页读取端口；本类不拥有或关闭 handle。 */
    private final PageStore pageStore;

    /** 实例固定页大小，决定物理读缓冲、extent 位图宽度和管理区跨度。 */
    private final PageSize pageSize;

    /** 与运行期 allocator、scrubber 共用的 XDES/bitmap 固定位置公式。 */
    private final ExtentManagementRegionLayout layout;

    /**
     * 创建只读普通准入校验器。
     *
     * @param pageStore 已由 DiskSpaceManager 打开目标文件的物理页端口；生命周期覆盖 loader
     * @param pageSize 实例固定页大小；必须与 page0 自描述一致
     * @throws DatabaseValidationException 任一依赖为空时抛出，loader 不得发布
     */
    MaterializedXdesAdmissionValidator(PageStore pageStore, PageSize pageSize) {
        if (pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("XDES admission pageStore/pageSize must not be null");
        }
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.layout = new ExtentManagementRegionLayout(pageSize);
    }

    /**
     * 验证 freeLimit 已发布范围所要求的管理页与管理 extent 账本。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>交叉核对 page0 currentSize/freeLimit、实例页大小与物理文件长度；逻辑范围不能指向文件外。</li>
     *     <li>若 extent0 已材料化，校验其系统 owner/空链形状，并在 group0 overflow 已启用时验证 page5 与 bit5。</li>
     *     <li>按 region 升序读取每个已跨越区的 primary、bitmap 和可选 overflow，严格校验 checksum、FIL identity
     *     与 XDES 专用 header；失败只留下原始磁盘证据。</li>
     *     <li>校验区首管理 descriptor 的 exact fixed bitmap，并证明三个全局 FLST base 的 first/last 均未引用
     *     该管理节点；全部成功后普通 loader 才可发布 metadata。</li>
     * </ol>
     *
     * @param spaceId 请求打开且已与 page0 自描述核对的稳定表空间标识
     * @param pageZero 已通过 FSP_HDR envelope 与 checksum/legacy 规则的 page0 raw 镜像；本方法只读
     * @param physical 从同一 page0 解出的物理字段；freeLimit 决定必须存在的管理区集合
     * @throws TablespaceCorruptedException 逻辑/物理范围、固定管理页、descriptor 或 FLST membership 不一致时抛出；
     *                                      调用方必须关闭普通打开的物理 handle，恢复路径可先执行 redo
     */
    void validate(SpaceId spaceId, ByteBuffer pageZero, SpaceHeaderPhysical physical) {
        // 1. page0 发布范围必须由当前文件完整承载；普通打开不代替 crash recovery 的文件扩展重对齐。
        long currentSize = physical.currentSizeInPages().value();
        long freeLimit = physical.freeLimitPageNo().value();
        long actualSize = pageStore.currentSizeInPages(spaceId).value();
        int pagesPerExtent = pageSize.pagesPerExtent();
        if (!physical.pageSize().equals(pageSize) || currentSize <= 0L || currentSize > actualSize
                || freeLimit > currentSize || freeLimit % pagesPerExtent != 0L) {
            throw corrupted("page0 XDES publication range is inconsistent: space=" + spaceId.value()
                    + " currentSize=" + currentSize + " actualSize=" + actualSize
                    + " freeLimit=" + freeLimit + " pagesPerExtent=" + pagesPerExtent);
        }
        long extentCount = freeLimit / pagesPerExtent;
        if (extentCount == 0L) {
            return;
        }

        // 2. extent0 是所有格式的系统管理域；group0 page5 只有真正被 descriptor 容量逼出时才是必需证据。
        ExtentDescriptorLocation systemLocation = layout.locate(ExtentId.of(spaceId, 0L));
        requireManagementShape(pageZero, systemLocation, "system extent0");
        requireNotGlobalListEndpoint(pageZero, systemLocation, "system extent0");
        if (extentCount > layout.entriesPerDescriptorPage() && layout.requiresOverflowPage(0L)) {
            long pageNo = layout.overflowXdesPageNo(0L).value();
            readAndValidateXdes(spaceId, pageNo, expectedXdesHeader(pageNo));
            requireAllocatedBit(pageZero, systemLocation, 5, "group0 overflow XDES page5");
        }

        // 3. 每个 freeLimit 已跨越的后续区都必须完整拥有 fixed pages；空 primary 也是已发布格式证据。
        long lastRegion = layout.regionIndexOfExtent(extentCount - 1L);
        for (long region = 1L; region <= lastRegion; region++) {
            long primaryPageNo = layout.primaryXdesPageNo(region).value();
            ByteBuffer primary = readAndValidateXdes(
                    spaceId, primaryPageNo, expectedXdesHeader(primaryPageNo));
            long bitmapPageNo = layout.bitmapPageNo(region).value();
            readAndValidateEnvelope(spaceId, bitmapPageNo, PageType.IBUF_BITMAP);

            ByteBuffer overflow = null;
            if (layout.requiresOverflowPage(region)) {
                long overflowPageNo = layout.overflowXdesPageNo(region).value();
                overflow = readAndValidateXdes(
                        spaceId, overflowPageNo, expectedXdesHeader(overflowPageNo));
            }

            // 4. fixed pages 先验证完毕，再消费 descriptor；失败没有任何 raw write、cache publish 或 FLST 修改。
            long managementExtentNo = primaryPageNo / pagesPerExtent;
            ExtentDescriptorLocation management = layout.locate(ExtentId.of(spaceId, managementExtentNo));
            ByteBuffer descriptorImage = descriptorImage(pageZero, primary, overflow,
                    management, primaryPageNo, region);
            requireCanonicalRegionDescriptor(descriptorImage, management, layout.requiresOverflowPage(region));
            requireNotGlobalListEndpoint(pageZero, management, "management extent " + managementExtentNo);
        }
    }

    /** 返回管理 descriptor 所在的已验证镜像；布局若指向当前区之外表示公式或持久范围损坏。 */
    private ByteBuffer descriptorImage(ByteBuffer pageZero, ByteBuffer primary, ByteBuffer overflow,
                                       ExtentDescriptorLocation location, long primaryPageNo, long region) {
        long descriptorPageNo = location.descriptorPageId().pageNo().value();
        if (descriptorPageNo == 0L) {
            return pageZero.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
        }
        if (descriptorPageNo == primaryPageNo) {
            return primary;
        }
        if (overflow != null && descriptorPageNo == layout.overflowXdesPageNo(region).value()) {
            return overflow;
        }
        throw corrupted("management descriptor points outside validated region pages: page="
                + descriptorPageNo + " region=" + region);
    }

    /** 严格读取 XDES 页，并在 FIL envelope 后交叉核对 role/group/range header。 */
    private ByteBuffer readAndValidateXdes(SpaceId spaceId, long pageNo, XdesPageHeader expected) {
        ByteBuffer page = readAndValidateEnvelope(spaceId, pageNo, PageType.XDES);
        if (page.getInt(XdesPageCodec.MAGIC_OFFSET) != XdesPageCodec.MAGIC
                || page.getInt(XdesPageCodec.FORMAT_OFFSET) != XdesPageCodec.FORMAT_VERSION
                || page.getInt(XdesPageCodec.ROLE_OFFSET) != expected.role().persistentCode()
                || page.getLong(XdesPageCodec.GROUP_BASE_OFFSET) != expected.groupBasePageNo()
                || page.getLong(XdesPageCodec.FIRST_EXTENT_OFFSET) != expected.firstExtentNo()
                || page.getInt(XdesPageCodec.ENTRY_COUNT_OFFSET) != expected.entryCount()) {
            throw corrupted("standalone XDES header mismatch: space=" + spaceId.value() + " page=" + pageNo);
        }
        return page;
    }

    /**
     * 定点读取必需管理页并严格校验 checksum、space/page identity 与 page type。独立管理页没有 legacy-zero
     * 兼容：它们只由当前 PAGE_INIT 格式产生，零页或未刷盘页必须先走恢复。
     */
    private ByteBuffer readAndValidateEnvelope(SpaceId spaceId, long pageNo, PageType expectedType) {
        ByteBuffer page = ByteBuffer.allocate(pageSize.bytes()).order(ByteOrder.BIG_ENDIAN);
        try {
            pageStore.readPage(PageId.of(spaceId, PageNo.of(pageNo)), page);
        } catch (RuntimeException failure) {
            throw new TablespaceCorruptedException("cannot read materialized management page: space="
                    + spaceId.value() + " page=" + pageNo, failure);
        }
        if (!PageImageChecksum.verify(page, pageSize)) {
            throw corrupted("materialized management page checksum/trailer mismatch: space="
                    + spaceId.value() + " page=" + pageNo);
        }
        long envelopePageNo = Integer.toUnsignedLong(page.getInt(PageEnvelopeLayout.PAGE_NO));
        if (page.getInt(PageEnvelopeLayout.SPACE_ID) != spaceId.value()
                || envelopePageNo != pageNo
                || page.getInt(PageEnvelopeLayout.PAGE_TYPE) != expectedType.code()) {
            throw corrupted("materialized management page identity/type mismatch: space="
                    + spaceId.value() + " page=" + pageNo + " type=" + expectedType);
        }
        return page.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
    }

    /** 按与 allocator 相同的固定公式构造独立 XDES 页 header。 */
    private XdesPageHeader expectedXdesHeader(long pageNo) {
        if (pageNo == 5L) {
            return new XdesPageHeader(XdesPageRole.OVERFLOW, 0L,
                    layout.entriesPerDescriptorPage(), checkedInt(layout.overflowEntryCount(0L), "entry count"));
        }
        if (pageNo > 0L && pageNo % pageSize.bytes() == 0L) {
            long region = pageNo / pageSize.bytes();
            return new XdesPageHeader(XdesPageRole.PRIMARY, pageNo,
                    layout.firstStandaloneExtent(region), checkedInt(layout.primaryEntryCount(region), "entry count"));
        }
        if (pageNo > 5L && (pageNo - 5L) % pageSize.bytes() == 0L) {
            long region = (pageNo - 5L) / pageSize.bytes();
            return new XdesPageHeader(XdesPageRole.OVERFLOW, pageNo - 5L,
                    checkedAdd(layout.firstStandaloneExtent(region), layout.entriesPerDescriptorPage(),
                            "overflow first extent"),
                    checkedInt(layout.overflowEntryCount(region), "entry count"));
        }
        throw corrupted("invalid standalone XDES formula page: " + pageNo);
    }

    /** 校验系统/管理 descriptor 的 state、owner 与空 prev/next 形状。 */
    private static void requireManagementShape(ByteBuffer image, ExtentDescriptorLocation location, String label) {
        int base = location.entryOffset();
        if (image.getInt(base + ExtentDescriptorLayout.STATE) != ExtentState.FSEG_FRAG.ordinal()
                || image.getLong(base + ExtentDescriptorLayout.OWNER_SEGMENT) != 0L
                || !isNullAddress(image, base + ExtentDescriptorLayout.PREV)
                || !isNullAddress(image, base + ExtentDescriptorLayout.NEXT)) {
            throw corrupted(label + " is not owner0/FSEG_FRAG with null list links");
        }
    }

    /** 后续区 descriptor 还必须精确只分配 primary、bitmap 与可选 overflow 固定页。 */
    private void requireCanonicalRegionDescriptor(ByteBuffer image, ExtentDescriptorLocation location,
                                                  boolean overflowRequired) {
        requireManagementShape(image, location, "management extent " + location.extentId().extentNo());
        int bitmap = location.entryOffset() + ExtentDescriptorLayout.BITMAP;
        for (int pageIndex = 0; pageIndex < pageSize.pagesPerExtent(); pageIndex++) {
            boolean actual = (image.get(bitmap + pageIndex / Byte.SIZE) & (1 << (pageIndex % Byte.SIZE))) != 0;
            boolean expected = pageIndex == 0 || pageIndex == 1 || (overflowRequired && pageIndex == 5);
            if (actual != expected) {
                throw corrupted("management extent fixed bitmap mismatch: extent="
                        + location.extentId().extentNo() + " pageIndex=" + pageIndex);
            }
        }
    }

    /** 校验 extent0 对延迟 group0 overflow 页的 allocation bit 已经发布。 */
    private static void requireAllocatedBit(ByteBuffer image, ExtentDescriptorLocation location,
                                            int pageIndex, String label) {
        int offset = location.entryOffset() + ExtentDescriptorLayout.BITMAP + pageIndex / Byte.SIZE;
        if ((image.get(offset) & (1 << (pageIndex % Byte.SIZE))) == 0) {
            throw corrupted(label + " is not allocated in system extent bitmap");
        }
    }

    /**
     * 管理 descriptor 的 prev/next 为空仍不足以证明它不在单节点链中；显式比较三个全局 base 的 first/last
     * 端点，堵住 length=1、节点双链接均为 NULL 的歧义。
     */
    private static void requireNotGlobalListEndpoint(ByteBuffer pageZero, ExtentDescriptorLocation location,
                                                     String label) {
        int[] bases = {
                SpaceHeaderLayout.FREE_EXTENT_LIST_BASE,
                SpaceHeaderLayout.FREE_FRAG_LIST_BASE,
                SpaceHeaderLayout.FULL_FRAG_LIST_BASE
        };
        for (int base : bases) {
            if (addressEquals(pageZero, base + FlstBaseLayout.FIRST, location)
                    || addressEquals(pageZero, base + FlstBaseLayout.LAST, location)) {
                throw corrupted(label + " is referenced by a global FLST base at offset " + base);
            }
        }
    }

    /** 比较 raw FileAddress 与 descriptor 的稳定 list-node 地址，不把全零 NULL 误当 page0 节点。 */
    private static boolean addressEquals(ByteBuffer page, int offset, ExtentDescriptorLocation location) {
        long rawPageNo = page.getLong(offset);
        int rawOffset = page.getInt(offset + Long.BYTES);
        return !(rawPageNo == 0L && rawOffset == 0)
                && rawPageNo == location.listNodeAddress().pageNo().value()
                && rawOffset == location.listNodeAddress().offset();
    }

    /** raw FileAddress 只有 12 字节全零才是 NULL；半零地址同样视为 descriptor 形状冲突。 */
    private static boolean isNullAddress(ByteBuffer page, int offset) {
        return page.getLong(offset) == 0L && page.getInt(offset + Long.BYTES) == 0;
    }

    /** 统一构造带表空间损坏语义的普通准入异常。 */
    private static TablespaceCorruptedException corrupted(String message) {
        return new TablespaceCorruptedException(message);
    }

    /** 以表空间损坏语义报告持久范围不能收窄为 XDES header int。 */
    private static int checkedInt(long value, String field) {
        try {
            return Math.toIntExact(value);
        } catch (ArithmeticException overflow) {
            throw new TablespaceCorruptedException("XDES " + field + " overflows: " + value, overflow);
        }
    }

    /** 以表空间损坏语义报告持久 extent 范围相加溢出。 */
    private static long checkedAdd(long left, long right, String field) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new TablespaceCorruptedException("XDES " + field + " overflows", overflow);
        }
    }
}
