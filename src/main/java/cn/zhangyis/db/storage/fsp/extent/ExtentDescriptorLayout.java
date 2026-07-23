package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;

/**
 * XDES entry 页内布局：page0 与独立 primary/overflow 页都从 SpaceHeaderLayout.XDES_BASE 起，
 * 每条 ENTRY_SIZE 字节。bitmap 固定保留 32 字节 stride，仅前 pagesPerExtent 位有效。
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

    /** page0 与独立 XDES 页统一保留到 256 字节后再放 descriptor，确保旧 page0 地址永久稳定。 */
    public static final int ENTRIES_BASE = SpaceHeaderLayout.XDES_BASE;

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
        try {
            return entryOffsetInPage(Math.toIntExact(extentNo));
        } catch (ArithmeticException error) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "XDES extent number cannot be represented as a page slot: " + extentNo, error);
        }
    }

    /**
     * 根据 descriptor 页内槽位计算 entry 起始偏移；与全局 extentNo 解耦，供独立 XDES 页复用。
     *
     * @param slotInPage 当前 descriptor 页内的零基槽位；必须非负且由管理区布局验证未超过容量
     * @return 从物理页首计算的 entry 字节偏移
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 槽位为负或乘加溢出时抛出
     */
    public static int entryOffsetInPage(int slotInPage) {
        if (slotInPage < 0) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "XDES slot in page must not be negative: " + slotInPage);
        }
        try {
            return Math.addExact(ENTRIES_BASE, Math.multiplyExact(slotInPage, ENTRY_SIZE));
        } catch (ArithmeticException error) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "XDES entry offset overflows for slot " + slotInPage, error);
        }
    }

    /**
     * 返回当前页大小下 bitmap 真正表达的字节数。小页 extent 仍保留 32 字节 stride，但未使用 padding 不得触碰 FIL trailer。
     *
     * @param pageSize 实例固定页大小；不得为空
     * @return {@code ceil(pagesPerExtent/8)}，范围为 8..32
     */
    public static int activeBitmapBytes(PageSize pageSize) {
        if (pageSize == null) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "XDES bitmap page size must not be null");
        }
        return (pageSize.pagesPerExtent() + Byte.SIZE - 1) / Byte.SIZE;
    }
}
