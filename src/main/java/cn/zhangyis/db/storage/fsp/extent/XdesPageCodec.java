package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;

/** 独立 XDES 页 header 的固定编解码器；page0 仍由 SpaceHeaderRepository 解释，不经过本类。 */
public final class XdesPageCodec {

    /** ASCII "XDES"；零页不会被误认为合法独立管理页。 */
    public static final int MAGIC = 0x58444553;
    /** 当前独立 XDES body 格式版本。 */
    public static final int FORMAT_VERSION = 1;
    /** body header 字段起点，紧随通用 FIL header。 */
    public static final int MAGIC_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
    /** format int。 */
    public static final int FORMAT_OFFSET = MAGIC_OFFSET + Integer.BYTES;
    /** role int。 */
    public static final int ROLE_OFFSET = FORMAT_OFFSET + Integer.BYTES;
    /** groupBasePageNo long。 */
    public static final int GROUP_BASE_OFFSET = ROLE_OFFSET + Integer.BYTES;
    /** firstExtentNo long。 */
    public static final int FIRST_EXTENT_OFFSET = GROUP_BASE_OFFSET + Long.BYTES;
    /** entryCount int，字段结束后到 256 保留为零。 */
    public static final int ENTRY_COUNT_OFFSET = FIRST_EXTENT_OFFSET + Long.BYTES;

    private XdesPageCodec() {
    }

    /**
     * 在已由 MTR 创建的 XDES 页上写入 envelope 与 body header。
     *
     * @param guard 新页的 X-latched guard；写入由其 MTR collector 收集 PAGE_BYTES
     * @param pageId 目标物理页 identity，必须与 guard 指向页一致
     * @param header 已由管理区布局计算的角色和 descriptor 范围
     */
    public static void write(PageGuard guard, PageId pageId, XdesPageHeader header) {
        if (guard == null || pageId == null || header == null) {
            throw new DatabaseValidationException("XDES codec write arguments must not be null");
        }
        PageEnvelope.writeHeader(guard, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.XDES));
        guard.writeInt(MAGIC_OFFSET, MAGIC);
        guard.writeInt(FORMAT_OFFSET, FORMAT_VERSION);
        guard.writeInt(ROLE_OFFSET, header.role().persistentCode());
        guard.writeLong(GROUP_BASE_OFFSET, header.groupBasePageNo());
        guard.writeLong(FIRST_EXTENT_OFFSET, header.firstExtentNo());
        guard.writeInt(ENTRY_COUNT_OFFSET, header.entryCount());
    }

    /**
     * 读取并交叉验证独立 XDES 页 identity、type 和 body header。
     *
     * @param guard 已持 S/X latch 的页面 guard
     * @param expectedPageId 调用方按 ExtentId 计算出的期望物理页
     * @param expectedHeader 调用方按固定管理区公式计算出的期望 header
     * @return 与页上字节一致的 header；成功时等于 expectedHeader
     * @throws FspMetadataException 任一 identity、版本、角色或范围不匹配时抛出，调用方不得继续修改该页
     */
    public static XdesPageHeader readAndValidate(
            PageGuard guard, PageId expectedPageId, XdesPageHeader expectedHeader) {
        if (guard == null || expectedPageId == null || expectedHeader == null) {
            throw new DatabaseValidationException("XDES codec read arguments must not be null");
        }
        FilePageHeader envelope;
        try {
            envelope = PageEnvelope.readHeader(guard);
        } catch (RuntimeException invalid) {
            throw new FspMetadataException("invalid XDES FIL envelope for " + expectedPageId, invalid);
        }
        if (!envelope.spaceId().equals(expectedPageId.spaceId())
                || envelope.pageNo() != expectedPageId.pageNo().value()
                || envelope.pageType() != PageType.XDES) {
            throw new FspMetadataException("XDES page identity/type mismatch: expected="
                    + expectedPageId + " actual=" + envelope);
        }
        if (guard.readInt(MAGIC_OFFSET) != MAGIC || guard.readInt(FORMAT_OFFSET) != FORMAT_VERSION) {
            throw new FspMetadataException("XDES page magic/format mismatch: " + expectedPageId);
        }
        XdesPageHeader actual = new XdesPageHeader(
                XdesPageRole.fromPersistentCode(guard.readInt(ROLE_OFFSET)),
                guard.readLong(GROUP_BASE_OFFSET),
                guard.readLong(FIRST_EXTENT_OFFSET),
                guard.readInt(ENTRY_COUNT_OFFSET));
        if (!actual.equals(expectedHeader)) {
            throw new FspMetadataException("XDES page header mismatch: expected="
                    + expectedHeader + " actual=" + actual);
        }
        return actual;
    }
}
