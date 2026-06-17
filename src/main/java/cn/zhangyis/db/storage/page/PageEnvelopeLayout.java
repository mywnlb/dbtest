package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.domain.PageSize;

/**
 * 物理页信封布局（设计 §5.3）：页首 FilePageHeader + 页尾 FilePageTrailer 的偏移与尺寸。单一真相来源，
 * 取代原 fsp 的 FilePageHeaderLayout/FilePageTrailerLayout/PageLayouts，供 buf 之上各层共用（buf 不依赖本类）。
 */
public final class PageEnvelopeLayout {

    private PageEnvelopeLayout() {
    }

    // ---- 页首 FilePageHeader（[0, FIL_PAGE_HEADER_BYTES)）----
    /** CRC32 校验和（派生，PageChecksum 盖）。 */
    public static final int CHECKSUM = 0;          // int 4
    public static final int SPACE_ID = 4;          // int 4
    public static final int PAGE_NO = 8;           // int 4
    public static final int PREV_PAGE_NO = 12;     // int 4
    public static final int NEXT_PAGE_NO = 16;     // int 4
    /** 页 LSN（恢复幂等用的 header LSN）。 */
    public static final int PAGE_LSN = 20;         // long 8
    public static final int PAGE_TYPE = 28;        // int 4 (ends 32; 32..37 预留)
    /** 页首总字节数（= 旧 PageLayouts.FIL_PAGE_DATA）。 */
    public static final int FIL_PAGE_HEADER_BYTES = 38;

    // ---- 页尾 FilePageTrailer（[pageSize-8, pageSize)）----
    public static final int FIL_PAGE_TRAILER_BYTES = 8;
    public static final int TRAILER_CHECKSUM = 0;  // int 4
    public static final int TRAILER_LOW32_LSN = 4; // int 4

    /** trailer 在页内的起始偏移。 */
    public static int trailerOffset(PageSize pageSize) {
        return pageSize.bytes() - FIL_PAGE_TRAILER_BYTES;
    }
}
