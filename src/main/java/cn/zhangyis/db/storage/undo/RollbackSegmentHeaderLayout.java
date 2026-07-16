package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * Rollback segment header 页（undo 表空间固定 page3）物理布局（设计 §6.3，0.3 简化版）。在 38 字节 FIL 信封头之后
 * 保存 rseg 元数据、active slot array 与 INSERT/UPDATE 两个 cached segment 栈。cached 栈以
 * {@code [0,count)} 连续数组落盘，既能在恢复后保持 LIFO，也避免仅靠内存目录造成 FSP inode 失去持久 owner。
 * v3 保存持久 history 双向链；v4 追加跨 kind 的 free undo segment FIFO base。
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
    /** 当前格式版本；旧版不含完整持久 owner 区，本实现不做在线迁移。 */
    static final int FORMAT_VERSION = 4;

    /** 魔数（u32），信封头之后第一字段。 */
    static final int MAGIC = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;  // 38
    /** 格式版本（u32）。 */
    static final int FORMAT = MAGIC + 4;                               // 42
    /** rollback segment id（u32）。 */
    static final int RSEG_ID = FORMAT + 4;                             // 46
    /** slot 容量（u32）；磁盘权威值，打开时校验与配置一致。 */
    static final int SLOT_CAPACITY = RSEG_ID + 4;                      // 50
    /** 每个 kind 的 cached segment 容量（u32）；0 表显式禁用缓存。 */
    static final int CACHE_CAPACITY_PER_KIND = SLOT_CAPACITY + 4;      // 54
    /** INSERT cached 栈当前元素数（u32）。 */
    static final int INSERT_CACHE_COUNT = CACHE_CAPACITY_PER_KIND + 4; // 58
    /** UPDATE cached 栈当前元素数（u32）。 */
    static final int UPDATE_CACHE_COUNT = INSERT_CACHE_COUNT + 4;      // 62
    /** 持久 history 链首页 pageNo（u64）；空链为 FIL_NULL。 */
    static final int HISTORY_HEAD_PAGE_NO = UPDATE_CACHE_COUNT + 4;    // 66
    /** 持久 history 链尾 pageNo（u64）；空链为 FIL_NULL。 */
    static final int HISTORY_TAIL_PAGE_NO = HISTORY_HEAD_PAGE_NO + 8;  // 74
    /** 当前持久 history 节点数（u64）。 */
    static final int HISTORY_LENGTH = HISTORY_TAIL_PAGE_NO + 8;        // 82
    /** 曾成功挂入 history 的最大 TransactionNo；purge 不回退该高水位。 */
    static final int LAST_TRANSACTION_NO = HISTORY_LENGTH + 8;         // 90
    /** 持久 free FIFO 首页 pageNo（u64）；空链为 FIL_NULL。 */
    static final int FREE_HEAD_PAGE_NO = LAST_TRANSACTION_NO + 8;      // 98
    /** 持久 free FIFO 尾页 pageNo（u64）；空链为 FIL_NULL。 */
    static final int FREE_TAIL_PAGE_NO = FREE_HEAD_PAGE_NO + 8;        // 106
    /** 持久 free FIFO 节点数（u64）；恢复期按此值做有界遍历。 */
    static final int FREE_LENGTH = FREE_TAIL_PAGE_NO + 8;              // 114
    /** active slot array 起点；每槽 u64 pageNo，{@code FIL_NULL}=空。 */
    static final int SLOT_ARRAY_BASE = FREE_LENGTH + 8;                // 122

    /** 第 idx 个 slot 的页内偏移。 */
    static int slotOffset(int idx) {
        return SLOT_ARRAY_BASE + idx * Long.BYTES;
    }

    /** 容纳 slotCapacity 个槽所需的末尾偏移（用于校验不越过页尾 trailer）。 */
    static int slotArrayEnd(int slotCapacity) {
        return SLOT_ARRAY_BASE + slotCapacity * Long.BYTES;
    }

    /** INSERT cached 栈数组起点，紧随 active slot array。 */
    static int insertCacheBase(int slotCapacity) {
        return slotArrayEnd(slotCapacity);
    }

    /** UPDATE cached 栈数组起点，紧随 INSERT cached 栈数组。 */
    static int updateCacheBase(int slotCapacity, int cacheCapacityPerKind) {
        return insertCacheBase(slotCapacity) + cacheCapacityPerKind * Long.BYTES;
    }

    /** 指定 kind/cache index 的页内偏移。 */
    static int cacheOffset(UndoLogKind kind, int slotCapacity, int cacheCapacityPerKind, int index) {
        int base = switch (kind) {
            case INSERT -> insertCacheBase(slotCapacity);
            case UPDATE -> updateCacheBase(slotCapacity, cacheCapacityPerKind);
            case TEMPORARY -> throw new UndoLogFormatException("temporary undo has no persistent cache stack");
        };
        return base + index * Long.BYTES;
    }

    /** page3 v4 全部定长数组结束位置。 */
    static int layoutEnd(int slotCapacity, int cacheCapacityPerKind) {
        return updateCacheBase(slotCapacity, cacheCapacityPerKind)
                + cacheCapacityPerKind * Long.BYTES;
    }
}
