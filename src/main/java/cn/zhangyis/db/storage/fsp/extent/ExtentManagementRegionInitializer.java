package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

/**
 * 在 freeLimit 越过重复管理区边界前格式化并保留 XDES/IBUF_BITMAP 固定页。
 *
 * <p>本类只编排 FSP 管理页，不扩展物理文件；调用方必须保证 page0 的 currentSize 与实际 PageStore 容量
 * 已覆盖待格式化页。所有页面先由同一 MTR 取得 page0 X gate，再按 primary、bitmap、overflow 的升序取得 X latch。
 * 全零页可以安全升级为管理页；非零页必须已经具有完全匹配的 identity/type/header，否则拒绝覆盖并要求离线重建。</p>
 */
public final class ExtentManagementRegionInitializer {

    /** 管理页 fix/latch 与 redo 字节收集入口。 */
    private final BufferPool pool;
    /** trailer 边界、管理区跨度和 extent 页数的实例级权威配置。 */
    private final PageSize pageSize;
    /** XDES canonical image 与固定页 bitmap 的唯一写入仓储。 */
    private final ExtentDescriptorRepository xdes;
    /** page0 兼容槽及重复管理页的纯寻址公式。 */
    private final ExtentManagementRegionLayout layout;

    /**
     * @param pool 共享 Buffer Pool；生命周期必须覆盖所有 FSP 分配
     * @param pageSize 实例固定页大小
     * @param xdes 与当前 pool/pageSize 配套的 XDES 仓储
     */
    public ExtentManagementRegionInitializer(BufferPool pool, PageSize pageSize,
                                             ExtentDescriptorRepository xdes) {
        if (pool == null || pageSize == null || xdes == null) {
            throw new DatabaseValidationException("management-region initializer dependencies must not be null");
        }
        if (!xdes.layout().pageSize().equals(pageSize)) {
            throw new DatabaseValidationException("management-region layout page size mismatch");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.xdes = xdes;
        this.layout = xdes.layout();
    }

    /**
     * 在材料化指定 extent 前补齐其依赖的管理页，并指出该 extent 是否属于不能进入普通 FREE 链的管理域。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 identity，并先取得 page0 X gate，统一串行化 freeLimit、管理页格式和跨页 FLST writer。</li>
     *     <li>extent0 直接视为 bootstrap 已保留；后续管理区首 extent 先格式化 primary、+1 bitmap 和可选 +5 overflow，
     *     再写 canonical 管理 descriptor。</li>
     *     <li>普通 extent 若首次落入 group0 overflow，则延迟格式化 page5 并在 extent0 bitmap 中保留该页。</li>
     *     <li>返回管理/普通分类；失败前不会覆盖非零未知页，MTR 回滚负责释放全部 latch/fix。</li>
     * </ol>
     *
     * @param mtr 当前 freeLimit 材料化 MTR
     * @param spaceId 目标表空间 identity
     * @param extentNo 即将由 freeLimit 消费的非负 extent 号
     * @return {@code true} 表示管理 extent 已保留且调用方只应推进 freeLimit；{@code false} 表示可继续 initFree
     * @throws FspMetadataException 固定位置已有非零业务页、错误管理页或冲突 descriptor 时抛出
     */
    public boolean prepareForMaterialization(MiniTransaction mtr, SpaceId spaceId, long extentNo) {
        // 1. page0 是该表空间全部管理区操作的 gate；任何独立页都只能在它之后获取。
        if (mtr == null || spaceId == null || extentNo < 0L) {
            throw new DatabaseValidationException("management-region MTR/space/extent must be valid");
        }
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);

        // 2. extent0 由 tablespace bootstrap 保留；其它区首在本次 freeLimit 消费前建立完整管理页集合。
        if (extentNo == 0L) {
            return true;
        }
        long region = layout.regionIndexOfExtent(extentNo);
        if (layout.isManagementExtent(extentNo)) {
            initializeRegion(mtr, spaceId, region, ExtentId.of(spaceId, extentNo));
            return true;
        }

        // 3. group0 的 page0 槽优先兼容旧格式，只有第一个超容量 extent 到来时才占用 page5。
        ExtentDescriptorLocation descriptor = layout.locate(ExtentId.of(spaceId, extentNo));
        if (region == 0L && descriptor.descriptorPageId().pageNo().value() == 5L) {
            ensureGroupZeroOverflow(mtr, spaceId);
        }

        // 4. 普通 extent 交回 FreeExtentService；独立 XDES header 仍会由 repository 在真正写槽前再次验证。
        return false;
    }

    /**
     * 格式化后续区的 primary/bitmap/overflow，并在最后发布管理 extent descriptor。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>若管理 descriptor 仍在 page0，先验证它未被 legacy owner/list 使用，避免远端页产生不可回滚修改。</li>
     *     <li>按 primary、bitmap、overflow 升序取得 X latch，并对整组页面只做全零/既有格式预检。</li>
     *     <li>所有页面通过后，为全零页登记 PAGE_INIT 并写 envelope/header；任何前置失败都不修改页面。</li>
     *     <li>最后写 management XDES canonical image，使固定页可由 bitmap/scrubber/recovery 共同识别。</li>
     * </ol>
     *
     * @param mtr 已持 page0 X gate 的材料化 MTR
     * @param spaceId 目标表空间 identity
     * @param region 正的重复管理区编号
     * @param managementExtent 该区首 extent identity
     */
    private void initializeRegion(MiniTransaction mtr, SpaceId spaceId, long region, ExtentId managementExtent) {
        // 1. page0 兼容槽中的旧 owner 必须在读取远端管理页前拒绝。
        if (region <= 0L) {
            throw new FspMetadataException("only extent0 may own management region 0");
        }
        PageId primaryId = PageId.of(spaceId, layout.primaryXdesPageNo(region));
        ExtentDescriptorLocation managementLocation = layout.locate(managementExtent);
        if (!managementLocation.descriptorPageId().equals(primaryId)) {
            xdes.requireManagementExtentReservable(mtr, managementExtent);
        }
        XdesPageHeader primaryHeader = new XdesPageHeader(XdesPageRole.PRIMARY,
                primaryId.pageNo().value(), layout.firstStandaloneExtent(region),
                checkedInt(layout.primaryEntryCount(region), "primary XDES entry count"));
        // 2. 先锁定并预检整组页面；此阶段只读字节，不写 header。
        PageId bitmapId = PageId.of(spaceId, layout.bitmapPageNo(region));
        PageGuard primaryGuard = mtr.getPage(pool, primaryId, PageLatchMode.EXCLUSIVE);
        boolean primaryBlank = validateXdesOrBlank(primaryGuard, primaryId, primaryHeader);
        PageGuard bitmapGuard = mtr.getPage(pool, bitmapId, PageLatchMode.EXCLUSIVE);
        boolean bitmapBlank = validateBitmapOrBlank(bitmapGuard, bitmapId);
        PageGuard overflowGuard = null;
        PageId overflowId = null;
        XdesPageHeader overflowHeader = null;
        boolean overflowBlank = false;
        if (layout.requiresOverflowPage(region)) {
            overflowId = PageId.of(spaceId, layout.overflowXdesPageNo(region));
            overflowHeader = new XdesPageHeader(XdesPageRole.OVERFLOW,
                    primaryId.pageNo().value(),
                    checkedAdd(layout.firstStandaloneExtent(region), layout.entriesPerDescriptorPage(),
                            "overflow XDES first extent"),
                    checkedInt(layout.overflowEntryCount(region), "overflow XDES entry count"));
            overflowGuard = mtr.getPage(pool, overflowId, PageLatchMode.EXCLUSIVE);
            overflowBlank = validateXdesOrBlank(overflowGuard, overflowId, overflowHeader);
        }

        if (!primaryBlank && managementLocation.descriptorPageId().equals(primaryId)) {
            xdes.requireManagementExtentReservable(mtr, managementExtent);
        }

        // 3. 所有固定位置都通过冲突预检后才写首个字节；MTR 无 content undo，不能边检查边格式化。
        if (primaryBlank) {
            mtr.recordBlankPageInitialization(primaryId, PageType.XDES);
            XdesPageCodec.write(primaryGuard, primaryId, primaryHeader);
        }
        if (bitmapBlank) {
            mtr.recordBlankPageInitialization(bitmapId, PageType.IBUF_BITMAP);
            writeBitmapEnvelope(bitmapGuard, bitmapId);
        }
        if (overflowBlank) {
            mtr.recordBlankPageInitialization(overflowId, PageType.XDES);
            XdesPageCodec.write(overflowGuard, overflowId, overflowHeader);
        }
        // 4. descriptor 是管理区已发布的最后证据；它不会进入普通 FLST。
        if (overflowGuard != null) {
            xdes.reserveManagementExtent(mtr, managementExtent, 0, 1, 5);
        } else {
            xdes.reserveManagementExtent(mtr, managementExtent, 0, 1);
        }
    }

    /** group0 首次使用独立槽时延迟占用 page5，兼容所有旧 page0 descriptor 物理地址。 */
    private void ensureGroupZeroOverflow(MiniTransaction mtr, SpaceId spaceId) {
        if (!layout.requiresOverflowPage(0L)) {
            throw new FspMetadataException("group0 overflow requested for a page size that does not require it");
        }
        xdes.requireManagementExtentReservable(mtr, ExtentId.of(spaceId, 0L));
        PageId pageId = PageId.of(spaceId, layout.overflowXdesPageNo(0L));
        XdesPageHeader header = new XdesPageHeader(XdesPageRole.OVERFLOW, 0L,
                layout.entriesPerDescriptorPage(),
                checkedInt(layout.overflowEntryCount(0L), "group0 overflow XDES entry count"));
        formatOrValidateXdes(mtr, pageId, header);
        xdes.markManagementPageAllocated(mtr, pageId);
    }

    /** 全零页写入 XDES envelope/header；已格式化页只验证，不清空现存 entries。 */
    private void formatOrValidateXdes(MiniTransaction mtr, PageId pageId, XdesPageHeader header) {
        PageGuard guard = mtr.getPage(pool, pageId, PageLatchMode.EXCLUSIVE);
        if (validateXdesOrBlank(guard, pageId, header)) {
            mtr.recordBlankPageInitialization(pageId, PageType.XDES);
            XdesPageCodec.write(guard, pageId, header);
        }
    }

    /** 返回页面是否全零；非零页必须是完全匹配的 XDES，且本方法不修改任何字节。 */
    private boolean validateXdesOrBlank(PageGuard guard, PageId pageId, XdesPageHeader header) {
        if (isAllZero(guard)) {
            return true;
        }
        XdesPageCodec.readAndValidate(guard, pageId, header);
        return false;
    }

    /** 返回页面是否全零；非零页必须是完全匹配的 IBUF_BITMAP，且本方法不修改任何字节。 */
    private boolean validateBitmapOrBlank(PageGuard guard, PageId pageId) {
        if (isAllZero(guard)) {
            return true;
        }
        FilePageHeader actual;
        try {
            actual = PageEnvelope.readHeader(guard);
        } catch (RuntimeException invalid) {
            throw new FspMetadataException("invalid repeated IBUF_BITMAP envelope: " + pageId, invalid);
        }
        if (!actual.spaceId().equals(pageId.spaceId()) || actual.pageNo() != pageId.pageNo().value()
                || actual.pageType() != PageType.IBUF_BITMAP) {
            throw new FspMetadataException("management bitmap page conflicts with existing content: expected="
                    + pageId + " actual=" + actual);
        }
        return false;
    }

    /** 在已完成整组冲突预检的全零页上写入 IBUF_BITMAP envelope。 */
    private static void writeBitmapEnvelope(PageGuard guard, PageId pageId) {
        PageEnvelope.writeHeader(guard, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.IBUF_BITMAP));
    }

    /** 只有物理扩展产生的完整零页才允许被在线格式化，未知非零证据一律保留。 */
    private boolean isAllZero(PageGuard guard) {
        byte[] image = guard.readBytes(0, pageSize.bytes());
        for (byte value : image) {
            if (value != 0) {
                return false;
            }
        }
        return true;
    }

    /** checked narrowing，异常保留具体管理字段上下文。 */
    private static int checkedInt(long value, String field) {
        try {
            return Math.toIntExact(value);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException(field + " cannot be represented as int: " + value, error);
        }
    }

    /** checked addition，异常转换为项目校验异常并保留根因。 */
    private static long checkedAdd(long left, long right, String field) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException(field + " overflows", error);
        }
    }
}
