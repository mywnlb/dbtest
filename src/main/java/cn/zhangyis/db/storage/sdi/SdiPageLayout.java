package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * GENERAL 表空间固定 page3 的 SDI v1 布局。所有偏移相对物理页首，body 不能覆盖 index-DDL footer
 * 与统一 FIL trailer。
 */
public final class SdiPageLayout {

    private SdiPageLayout() {
    }

    /** extent0 已保留的 SDI 固定页号。 */
    public static final long PAGE_NO = 3L;
    /** ASCII `SDI1`。
     *
     * 持久结构布局常量；它定义 {@code SdiPageLayout} 中 {@code MAGIC} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int MAGIC = 0x53444931;
    /** 单页 SDI 格式版本。 */
    public static final int FORMAT_VERSION = 1;
    /**
     * 持久结构布局常量；它定义 {@code SdiPageLayout} 中 {@code MAGIC_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int MAGIC_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
    /**
     * 持久结构布局常量；它定义 {@code SdiPageLayout} 中 {@code FORMAT_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int FORMAT_OFFSET = MAGIC_OFFSET + Integer.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code SdiPageLayout} 中 {@code TABLE_ID_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int TABLE_ID_OFFSET = FORMAT_OFFSET + Integer.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code SdiPageLayout} 中 {@code DICTIONARY_VERSION_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int DICTIONARY_VERSION_OFFSET = TABLE_ID_OFFSET + Long.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code SdiPageLayout} 中 {@code PAYLOAD_LENGTH_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PAYLOAD_LENGTH_OFFSET = DICTIONARY_VERSION_OFFSET + Long.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code SdiPageLayout} 中 {@code PAYLOAD_CRC32C_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PAYLOAD_CRC32C_OFFSET = PAYLOAD_LENGTH_OFFSET + Integer.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code SdiPageLayout} 中 {@code PAYLOAD_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PAYLOAD_OFFSET = PAYLOAD_CRC32C_OFFSET + Integer.BYTES;
    /** CREATE/DROP INDEX 崩溃恢复 descriptor 共用的固定页尾保留区。 */
    public static final int INDEX_BUILD_FOOTER_BYTES = 96;
    /** footer magic：ASCII `IDX1`。全零 footer 表示没有未决索引 DDL。 */
    public static final int INDEX_BUILD_MAGIC = 0x49445831;
    /** 旧 CREATE INDEX footer 版本；没有 action 字段，兼容读取时固定解释为 BUILD。 */
    public static final int LEGACY_INDEX_BUILD_FORMAT_VERSION = 1;
    /** 当前索引 DDL footer 版本；在 version 后显式持久化 BUILD/DROP action。 */
    public static final int INDEX_DDL_FORMAT_VERSION = 2;

    /**
     * 计算当前实例单页最多容纳的 opaque DD payload。
     *
     * @param pageSize 实例固定页大小
     * @return 不覆盖 FIL trailer 的非负 payload 容量
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static int indexBuildFooterOffset(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("SDI footer page size must not be null");
        }
        return PageEnvelopeLayout.trailerOffset(pageSize) - INDEX_BUILD_FOOTER_BYTES;
    }

    /**
     * 旧 page3 没有 footer 时可使用的完整容量，只供兼容读取旧 SDI，任何新写入不得使用。
     *
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code legacyPayloadCapacity} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static int legacyPayloadCapacity(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("legacy SDI capacity page size must not be null");
        }
        return PageEnvelopeLayout.trailerOffset(pageSize) - PAYLOAD_OFFSET;
    }
}
