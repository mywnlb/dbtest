package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * GENERAL 表空间固定 page3 的 SDI v1 布局。所有偏移相对物理页首，body 不能覆盖 index-build footer
 * 与统一 FIL trailer。
 */
public final class SdiPageLayout {

    private SdiPageLayout() {
    }

    /** extent0 已保留的 SDI 固定页号。 */
    public static final long PAGE_NO = 3L;
    /** ASCII `SDI1`。 */
    public static final int MAGIC = 0x53444931;
    /** 单页 SDI 格式版本。 */
    public static final int FORMAT_VERSION = 1;
    public static final int MAGIC_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
    public static final int FORMAT_OFFSET = MAGIC_OFFSET + Integer.BYTES;
    public static final int TABLE_ID_OFFSET = FORMAT_OFFSET + Integer.BYTES;
    public static final int DICTIONARY_VERSION_OFFSET = TABLE_ID_OFFSET + Long.BYTES;
    public static final int PAYLOAD_LENGTH_OFFSET = DICTIONARY_VERSION_OFFSET + Long.BYTES;
    public static final int PAYLOAD_CRC32C_OFFSET = PAYLOAD_LENGTH_OFFSET + Integer.BYTES;
    public static final int PAYLOAD_OFFSET = PAYLOAD_CRC32C_OFFSET + Integer.BYTES;
    /** CREATE INDEX 崩溃回收 descriptor 的固定页尾保留区。 */
    public static final int INDEX_BUILD_FOOTER_BYTES = 96;
    /** footer magic：ASCII `IDX1`。全零 footer 表示没有未决 build。 */
    public static final int INDEX_BUILD_MAGIC = 0x49445831;
    /** footer 固定格式版本。 */
    public static final int INDEX_BUILD_FORMAT_VERSION = 1;

    /**
     * 计算当前实例单页最多容纳的 opaque DD payload。
     *
     * @param pageSize 实例固定页大小
     * @return 不覆盖 FIL trailer 的非负 payload 容量
     */
    public static int payloadCapacity(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("SDI payload capacity page size must not be null");
        }
        return indexBuildFooterOffset(pageSize) - PAYLOAD_OFFSET;
    }

    /**
     * 返回 footer 的物理起始偏移；SDI payload 和 footer 永不重叠。
     *
     * @param pageSize 实例固定页大小
     * @return trailer 前固定 96 字节区域的起点
     */
    public static int indexBuildFooterOffset(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("SDI footer page size must not be null");
        }
        return PageEnvelopeLayout.trailerOffset(pageSize) - INDEX_BUILD_FOOTER_BYTES;
    }

    /**
     * 旧 page3 没有 footer 时可使用的完整容量，只供兼容读取旧 SDI，任何新写入不得使用。
     */
    public static int legacyPayloadCapacity(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("legacy SDI capacity page size must not be null");
        }
        return PageEnvelopeLayout.trailerOffset(pageSize) - PAYLOAD_OFFSET;
    }
}
