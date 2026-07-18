package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.flst.FlstBaseLayout;


import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * SegmentInode entry 布局（page 2，从 INODE_BASE 起，每条 ENTRY_SIZE 字节）。used==0 空闲槽；fragment 槽空哨兵=0。
 * 三个 extent list 头为 FLST base（32B）。
 */
public final class SegmentInodeLayout {

    private SegmentInodeLayout() {
    }

    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code INODE_BASE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int INODE_BASE = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES; // 38

    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code USED} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int USED = 0;                          // int（0=空闲,1=在用）
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code SEGMENT_ID} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int SEGMENT_ID = USED + 4;             // 4 long
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code PURPOSE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PURPOSE = SEGMENT_ID + 8;          // 12 int（SegmentPurpose ordinal）
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code USED_PAGE_COUNT} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int USED_PAGE_COUNT = PURPOSE + 4;     // 16 long
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code RESERVED_PAGE_COUNT} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int RESERVED_PAGE_COUNT = USED_PAGE_COUNT + 8; // 24 long
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code FREE_EXTENT_LIST_BASE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int FREE_EXTENT_LIST_BASE = RESERVED_PAGE_COUNT + 8;            // 32 FlstBase(32)
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code NOT_FULL_EXTENT_LIST_BASE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int NOT_FULL_EXTENT_LIST_BASE = FREE_EXTENT_LIST_BASE + FlstBaseLayout.SIZE; // 64
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code FULL_EXTENT_LIST_BASE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int FULL_EXTENT_LIST_BASE = NOT_FULL_EXTENT_LIST_BASE + FlstBaseLayout.SIZE; // 96
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code FRAGMENT_SLOTS} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int FRAGMENT_SLOTS = FULL_EXTENT_LIST_BASE + FlstBaseLayout.SIZE;            // 128
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code FRAGMENT_SLOT_COUNT} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int FRAGMENT_SLOT_COUNT = 32;
    /**
     * 持久结构布局常量；它定义 {@code SegmentInodeLayout} 中 {@code ENTRY_SIZE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int ENTRY_SIZE = FRAGMENT_SLOTS + FRAGMENT_SLOT_COUNT * 8; // 384

    /**
     * 计算一个 INODE 页在固定页头之后能够容纳的完整 inode entry 数量；尾部不足一个 entry 的字节不计入结果。
     *
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code maxInodesInPage} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public static long maxInodesInPage(PageSize pageSize) {
        return (long) (pageSize.bytes() - INODE_BASE) / ENTRY_SIZE;
    }

    /**
     * 计算 {@code slotOffset} 所表达的表空间、区与段分配数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param inodeSlot 参与 {@code slotOffset} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code slotOffset} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public static int slotOffset(int inodeSlot) {
        return INODE_BASE + inodeSlot * ENTRY_SIZE;
    }

    /**
     * 计算 {@code fragmentSlotOffset} 所表达的表空间、区与段分配数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param inodeSlot 参与 {@code fragmentSlotOffset} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param fragIdx 参与 {@code fragmentSlotOffset} 的原始数值身份 {@code fragIdx}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @return {@code fragmentSlotOffset} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public static int fragmentSlotOffset(int inodeSlot, int fragIdx) {
        return slotOffset(inodeSlot) + FRAGMENT_SLOTS + fragIdx * 8;
    }
}
