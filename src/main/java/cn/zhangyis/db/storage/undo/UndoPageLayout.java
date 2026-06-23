package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * undo page 物理布局（T1.3b 头部拆分）。page header {@code [38,63)} 每页都有，保存页内追加游标、
 * segment 归属和 first 页标志；undo log header {@code [63,97)} 仅 first 页有语义，非 first 页仍预留并清零。
 *
 * <p>所有 undo 页统一 {@link #RECORD_AREA_START}=97，换来 RollPointer.offset 与页内 record 槽解析不区分
 * first/chain 页。页链前后指针复用 FIL header 的 prev/next，不在本布局中重复保存。
 */
final class UndoPageLayout {

    private UndoPageLayout() {
    }

    /** 下一条 record 追加位置（u16）；format 初始化为 {@link #RECORD_AREA_START}。 */
    static final int FREE_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES; // 38
    /** 本页已追加 undo record 数（u16），只统计当前页。 */
    static final int RECORD_COUNT = FREE_OFFSET + 2;                         // 40
    /** 本页最近一条 record 的 undoNo（u64，0 表示本页空）。 */
    static final int PAGE_LAST_UNDO_NO = RECORD_COUNT + 2;                   // 42
    /** 所属 UNDO segment id（u64），用于 reopen 重建 handle 与 roll pointer 段一致性校验。 */
    static final int SEGMENT_ID = PAGE_LAST_UNDO_NO + 8;                     // 50
    /** FSP segment inode 槽（u32），与 segment id 一起定位续分配所需的 SegmentRef。 */
    static final int INODE_SLOT = SEGMENT_ID + 8;                            // 58
    /** 页标志（u8）：bit0 表示 first page；其余位预留，当前格式写 0。 */
    static final int PAGE_FLAGS = INODE_SLOT + 4;                            // 62
    /** page header 末尾，也是 undo log header 起点。 */
    static final int PAGE_HEADER_END = PAGE_FLAGS + 1;                       // 63

    /** undo log 所属事务 id（u64），仅 first 页有效。 */
    static final int TRANSACTION_ID = PAGE_HEADER_END;                       // 63
    /** UndoLogKind ordinal（u8），本片只使用 INSERT。 */
    static final int UNDO_KIND = TRANSACTION_ID + 8;                         // 71
    /** undo log 状态占位（u8），本片恒 ACTIVE。 */
    static final int STATE = UNDO_KIND + 1;                                  // 72
    /** 链首页号（u32），first 页上等于自身页号。 */
    static final int FIRST_PAGE_NO = STATE + 1;                              // 73
    /** 当前链尾页号（u32），append 生长时推进。 */
    static final int LAST_PAGE_NO = FIRST_PAGE_NO + 4;                       // 77
    /** 整条 undo log 链的 record 总数（u64）。 */
    static final int LOG_RECORD_COUNT = LAST_PAGE_NO + 4;                    // 81
    /** 整条 undo log 链最近一条 record 的 undoNo（u64）。 */
    static final int LOG_LAST_UNDO_NO = LOG_RECORD_COUNT + 8;                // 89
    /** undo log header 末尾。 */
    static final int LOG_HEADER_END = LOG_LAST_UNDO_NO + 8;                  // 97

    /** record area 起点；所有页一致，record 槽格式为 [len u16][payload]。 */
    static final int RECORD_AREA_START = LOG_HEADER_END;

    /** undo log 状态占位常量：ACTIVE。 */
    static final int STATE_ACTIVE = 0;
    /** {@link #PAGE_FLAGS} bit0：first page 标志。 */
    static final int FLAG_FIRST_PAGE = 0x01;
}
