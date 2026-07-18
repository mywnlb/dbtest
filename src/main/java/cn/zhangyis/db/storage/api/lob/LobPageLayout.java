package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/** BLOB 页 body 的稳定 v1 布局；页首/页尾继续使用统一 FIL envelope。 */
public final class LobPageLayout {

    private LobPageLayout() {
    }

    /** ASCII "LOB1"，用于区别未格式化或错误复用的已分配页。 */
    public static final int MAGIC_VALUE = 0x4C4F4231;
    /** v1 body 版本；布局变化必须追加版本并保留旧解码。 */
    public static final int VERSION_VALUE = 1;
    /**
     * 持久结构布局常量；它定义 {@code LobPageLayout} 中 {@code MAGIC} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int MAGIC = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
    /**
     * 持久结构布局常量；它定义 {@code LobPageLayout} 中 {@code VERSION} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int VERSION = MAGIC + Integer.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code LobPageLayout} 中 {@code CHUNK_INDEX} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int CHUNK_INDEX = VERSION + Integer.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code LobPageLayout} 中 {@code CHUNK_LENGTH} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int CHUNK_LENGTH = CHUNK_INDEX + Integer.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code LobPageLayout} 中 {@code SEGMENT_ID} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int SEGMENT_ID = CHUNK_LENGTH + Integer.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code LobPageLayout} 中 {@code INODE_SLOT} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int INODE_SLOT = SEGMENT_ID + Long.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code LobPageLayout} 中 {@code TOTAL_LENGTH} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int TOTAL_LENGTH = INODE_SLOT + Integer.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code LobPageLayout} 中 {@code WHOLE_CRC32} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int WHOLE_CRC32 = TOTAL_LENGTH + Integer.BYTES;
    /**
     * 持久结构布局常量；它定义 {@code LobPageLayout} 中 {@code PAGE_COUNT} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PAGE_COUNT = WHOLE_CRC32 + Integer.BYTES;
    /** chunk payload 首字节。 */
    public static final int DATA = PAGE_COUNT + Integer.BYTES;

    /** 计算单页最大 chunk，排除统一 trailer；过小页配置 fail-closed。
     *
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code payloadCapacity} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static int payloadCapacity(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("LOB page size must not be null");
        }
        int capacity = PageEnvelopeLayout.trailerOffset(pageSize) - DATA;
        if (capacity <= 0) {
            throw new DatabaseValidationException("page size too small for LOB layout: " + pageSize.bytes());
        }
        return capacity;
    }
}
