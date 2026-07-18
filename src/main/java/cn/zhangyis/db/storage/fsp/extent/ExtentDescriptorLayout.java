package cn.zhangyis.db.storage.fsp.extent;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;


import cn.zhangyis.db.domain.PageSize;

/**
 * XDES entry 布局：内嵌 page 0、从 SpaceHeaderLayout.XDES_BASE 起、每条 ENTRY_SIZE 字节（条内偏移如下）。
 * bitmap 固定 32 字节（256 位，1 位/页，1=已分配），仅前 pagesPerExtent 位有效。
 */
public final class ExtentDescriptorLayout {

    private ExtentDescriptorLayout() {
    }

    /**
     * 持久结构布局常量；它定义 {@code ExtentDescriptorLayout} 中 {@code STATE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int STATE = 0;                 // int（ExtentState ordinal）
    /**
     * 持久结构布局常量；它定义 {@code ExtentDescriptorLayout} 中 {@code OWNER_SEGMENT} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int OWNER_SEGMENT = STATE + 4; // 4 long（0=无主）
    /**
     * 持久结构布局常量；它定义 {@code ExtentDescriptorLayout} 中 {@code PREV} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PREV = OWNER_SEGMENT + 8;  // 12 FileAddress(12)
    /**
     * 持久结构布局常量；它定义 {@code ExtentDescriptorLayout} 中 {@code NEXT} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int NEXT = PREV + 12;          // 24 FileAddress(12)
    /**
     * 持久结构布局常量；它定义 {@code ExtentDescriptorLayout} 中 {@code BITMAP} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int BITMAP = NEXT + 12;        // 36
    /**
     * 持久结构布局常量；它定义 {@code ExtentDescriptorLayout} 中 {@code BITMAP_BYTES} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int BITMAP_BYTES = 32;         // 256 位
    /**
     * 持久结构布局常量；它定义 {@code ExtentDescriptorLayout} 中 {@code ENTRY_SIZE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int ENTRY_SIZE = BITMAP + BITMAP_BYTES; // 68

    /**
     * 计算 page 0 在 Space Header 与固定 XDES entry 布局之后能够容纳的完整 extent descriptor 数量。
     *
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code maxEntriesInPage0} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public static long maxEntriesInPage0(PageSize pageSize) {
        return (long) (pageSize.bytes() - SpaceHeaderLayout.XDES_BASE) / ENTRY_SIZE;
    }

    /**
     * 计算 {@code entryOffset} 所表达的表空间、区与段分配数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param extentNo 参与 {@code entryOffset} 的原始数值身份 {@code extentNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @return {@code entryOffset} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public static int entryOffset(long extentNo) {
        return Math.toIntExact(SpaceHeaderLayout.XDES_BASE + extentNo * ENTRY_SIZE);
    }
}
