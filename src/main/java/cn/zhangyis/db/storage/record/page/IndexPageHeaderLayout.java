package cn.zhangyis.db.storage.record.page;

/**
 * INDEX page header 字段偏移（innodb-record-design §7 PAGE HEADER 教学子集，紧跟 §5.3 信封头）。
 * 除 {@link #INDEX_ID}（u64）外均为大端 u16，整体占 {@code [38,66)}。
 *
 * <p>未建模字段（教学简化，标注后续片）：PAGE_MAX_TRX_ID（MVCC，trx 暂停）、PAGE_BTR_SEG_LEAF/TOP（段指针，归 fsp/btree）、
 * PAGE_N_HEAP 最高位的行格式位（只用 compact 风格 forward layout）。
 */
final class IndexPageHeaderLayout {

    private IndexPageHeaderLayout() {
    }

    /** PageDirectory 槽数（u16）。 */
    static final int N_DIR_SLOTS = IndexPageLayout.PAGE_HEADER_START; // 38
    /** heap 顶（首个空闲字节偏移，u16）。 */
    static final int HEAP_TOP = N_DIR_SLOTS + 2;     // 40
    /** heap 记录总数，含 infimum/supremum（u16）。 */
    static final int N_HEAP = HEAP_TOP + 2;          // 42
    /** GarbageList 头记录偏移，0=空（u16）。 */
    static final int FREE = N_HEAP + 2;              // 44
    /** 删除记录占用字节总数（u16）。 */
    static final int GARBAGE = FREE + 2;             // 46
    /** 上次插入记录偏移，0=无（u16）。 */
    static final int LAST_INSERT = GARBAGE + 2;      // 48
    /** 插入方向 code（u16）。 */
    static final int DIRECTION = LAST_INSERT + 2;    // 50
    /** 同方向连续插入计数（u16）。 */
    static final int N_DIRECTION = DIRECTION + 2;    // 52
    /** 用户记录数，不含 infimum/supremum，含 delete-marked（u16）。 */
    static final int N_RECS = N_DIRECTION + 2;       // 54
    /** B+Tree 层，0=leaf（u16）。 */
    static final int LEVEL = N_RECS + 2;             // 56
    /** 索引 id（u64，占 8 字节，结束于 66）。 */
    static final int INDEX_ID = LEVEL + 2;           // 58
}
