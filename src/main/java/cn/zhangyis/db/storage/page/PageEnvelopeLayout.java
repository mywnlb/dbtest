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
    /**
     * 持久结构布局常量；它定义 {@code PageEnvelopeLayout} 中 {@code SPACE_ID} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int SPACE_ID = 4;          // int 4
    /**
     * 持久结构布局常量；它定义 {@code PageEnvelopeLayout} 中 {@code PAGE_NO} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PAGE_NO = 8;           // int 4
    /**
     * 持久结构布局常量；它定义 {@code PageEnvelopeLayout} 中 {@code PREV_PAGE_NO} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PREV_PAGE_NO = 12;     // int 4
    /**
     * 持久结构布局常量；它定义 {@code PageEnvelopeLayout} 中 {@code NEXT_PAGE_NO} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int NEXT_PAGE_NO = 16;     // int 4
    /** 页 LSN（恢复幂等用的 header LSN）。 */
    public static final int PAGE_LSN = 20;         // long 8
    /**
     * 持久结构布局常量；它定义 {@code PageEnvelopeLayout} 中 {@code PAGE_TYPE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PAGE_TYPE = 28;        // int 4 (ends 32; 32..37 预留)
    /** 页首总字节数（= 旧 PageLayouts.FIL_PAGE_DATA）。 */
    public static final int FIL_PAGE_HEADER_BYTES = 38;

    // ---- 页尾 FilePageTrailer（[pageSize-8, pageSize)）----
    /**
     * 持久结构布局常量；它定义 {@code PageEnvelopeLayout} 中 {@code FIL_PAGE_TRAILER_BYTES} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int FIL_PAGE_TRAILER_BYTES = 8;
    /**
     * 持久结构布局常量；它定义 {@code PageEnvelopeLayout} 中 {@code TRAILER_CHECKSUM} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int TRAILER_CHECKSUM = 0;  // int 4
    /**
     * 持久结构布局常量；它定义 {@code PageEnvelopeLayout} 中 {@code TRAILER_LOW32_LSN} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int TRAILER_LOW32_LSN = 4; // int 4

    /** trailer 在页内的起始偏移。
     *
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code trailerOffset} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public static int trailerOffset(PageSize pageSize) {
        return pageSize.bytes() - FIL_PAGE_TRAILER_BYTES;
    }
}
