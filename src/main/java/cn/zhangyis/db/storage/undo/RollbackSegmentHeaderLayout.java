package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * Rollback segment header 页（undo 表空间固定 page3）物理布局（设计 §6.3，0.3 简化版）。在 38 字节 FIL 信封头之后
 * 保存 rseg 元数据 + slot array（slot -> insert-undo segment 首页号）。本片只持久 slot 目录，§6.3 的 history list
 * base / cached·free segment list / lastTransactionNo 留后续片。
 *
 * <p>page3 由 {@code ExtentDescriptorRepository.reserveSystemExtent} 预留为系统页（page0..3），不会被 undo segment
 * 分配占用，故可作固定 rseg header 家（同 page0=FSP_HDR / page2=INODE 的约定）。
 */
final class RollbackSegmentHeaderLayout {

    private RollbackSegmentHeaderLayout() {
    }

    /** undo 表空间内 rseg header 固定页号。 */
    static final long RSEG_HEADER_PAGE_NO = 3;

    /** 魔数 "RSEG"，区分未格式化/错页。 */
    static final int MAGIC_VALUE = 0x52534547;
    /** 格式版本。 */
    static final int FORMAT_VERSION = 1;

    /** 魔数（u32），信封头之后第一字段。 */
    static final int MAGIC = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;  // 38
    /** 格式版本（u32）。 */
    static final int FORMAT = MAGIC + 4;                               // 42
    /** rollback segment id（u32）。 */
    static final int RSEG_ID = FORMAT + 4;                             // 46
    /** slot 容量（u32）；磁盘权威值，打开时校验与配置一致。 */
    static final int SLOT_CAPACITY = RSEG_ID + 4;                      // 50
    /** slot array 起点；每槽 u64 pageNo，{@code FIL_NULL}=空。 */
    static final int SLOT_ARRAY_BASE = SLOT_CAPACITY + 4;              // 54

    /** 第 idx 个 slot 的页内偏移。 */
    static int slotOffset(int idx) {
        return SLOT_ARRAY_BASE + idx * Long.BYTES;
    }

    /** 容纳 slotCapacity 个槽所需的末尾偏移（用于校验不越过页尾 trailer）。 */
    static int slotArrayEnd(int slotCapacity) {
        return SLOT_ARRAY_BASE + slotCapacity * Long.BYTES;
    }
}
