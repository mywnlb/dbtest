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
    /** rollback segment id（u32）。
     *
     * 持久结构布局常量；它定义 {@code RollbackSegmentHeaderLayout} 中 {@code RSEG_ID} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
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

    /** 第 idx 个 slot 的页内偏移。
     *
     * @param idx 参与 {@code slotOffset} 的原始数值身份 {@code idx}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @return {@code slotOffset} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    static int slotOffset(int idx) {
        return SLOT_ARRAY_BASE + idx * Long.BYTES;
    }

    /** 容纳 slotCapacity 个槽所需的末尾偏移（用于校验不越过页尾 trailer）。
     *
     * @param slotCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code slotArrayEnd} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    static int slotArrayEnd(int slotCapacity) {
        return SLOT_ARRAY_BASE + slotCapacity * Long.BYTES;
    }

    /** INSERT cached 栈数组起点，紧随 active slot array。
     *
     * @param slotCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code insertCacheBase} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    static int insertCacheBase(int slotCapacity) {
        return slotArrayEnd(slotCapacity);
    }

    /** UPDATE cached 栈数组起点，紧随 INSERT cached 栈数组。
     *
     * @param slotCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param cacheCapacityPerKind 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code updateCacheBase} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    static int updateCacheBase(int slotCapacity, int cacheCapacityPerKind) {
        return insertCacheBase(slotCapacity) + cacheCapacityPerKind * Long.BYTES;
    }

    /** 指定 kind/cache index 的页内偏移。
     *
     * @param kind 选择 {@code cacheOffset} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param slotCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param cacheCapacityPerKind 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param index 参与 {@code cacheOffset} 的零基位置 {@code index}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code cacheOffset} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static int cacheOffset(UndoLogKind kind, int slotCapacity, int cacheCapacityPerKind, int index) {
        int base = switch (kind) {
            case INSERT -> insertCacheBase(slotCapacity);
            case UPDATE -> updateCacheBase(slotCapacity, cacheCapacityPerKind);
            case TEMPORARY -> throw new UndoLogFormatException("temporary undo has no persistent cache stack");
        };
        return base + index * Long.BYTES;
    }

    /** page3 v4 全部定长数组结束位置。
     *
     * @param slotCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param cacheCapacityPerKind 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code layoutEnd} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    static int layoutEnd(int slotCapacity, int cacheCapacityPerKind) {
        return updateCacheBase(slotCapacity, cacheCapacityPerKind)
                + cacheCapacityPerKind * Long.BYTES;
    }
}
