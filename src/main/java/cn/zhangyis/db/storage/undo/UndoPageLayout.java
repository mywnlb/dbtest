package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * undo page 物理布局（T1.3b 头部拆分，R 1.3 提交序号，1.4b 持久逻辑头）。page header {@code [38,63)}
 * 每页都有，保存页内追加游标、segment 归属、first 标志和格式版本；undo log header {@code [63,136)}
 * 仅 first 页有语义，非 first 页仍预留并清零。
 *
 * <p>所有 v3 undo 页统一 {@link #RECORD_AREA_START}=136，换来 RollPointer.offset 与页内 record 槽解析不区分
 * first/chain 页。v3 还把 {@link #UNDO_KIND} 复制到每张普通 UNDO 页，使只持 chain-page RollPointer 的 MVCC
 * 读取也能在本页完成类型校验。{@link #LOGICAL_LAST_UNDO_NO} 与 {@link #LOGICAL_HEAD_ROLL_POINTER} 连续组成
 * 15 字节持久逻辑头；
 * 它不替代 {@link #LOG_LAST_UNDO_NO} 物理 append 高水位。页链前后指针复用 FIL header 的 prev/next。
 *
 * <p><b>格式兼容性</b>：本教学实现不在线迁移旧版 {@code RECORD_AREA_START=105/120} 的 undo 文件。每张页都在
 * {@link #PAGE_FLAGS} 编码版本，打开旧版或未知版本必须 fail-closed；这是有意简化，与 MySQL/InnoDB 磁盘格式不兼容。
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
    /** 页标志（u8）：bit0 表示 first page；bits1..7 编码 undo 页格式版本。 */
    static final int PAGE_FLAGS = INODE_SLOT + 4;                            // 62
    /** page header 末尾，也是 undo log header 起点。 */
    static final int PAGE_HEADER_END = PAGE_FLAGS + 1;                       // 63

    /** undo log 所属事务 id（u64），仅 first 页有效。 */
    static final int TRANSACTION_ID = PAGE_HEADER_END;                       // 63
    /** UndoLogKind ordinal（u8）；v3 在 first/chain 页都有效，ordinal 顺序不可改变。 */
    static final int UNDO_KIND = TRANSACTION_ID + 8;                         // 71
    /** undo log 状态（u8）：ACTIVE/COMMITTED/CACHED，恢复期据此区分 rollback、history 与可复用 owner。 */
    static final int STATE = UNDO_KIND + 1;                                  // 72
    /** 链首页号（u32），first 页上等于自身页号。 */
    static final int FIRST_PAGE_NO = STATE + 1;                              // 73
    /** 当前链尾页号（u32），append 生长时推进。 */
    static final int LAST_PAGE_NO = FIRST_PAGE_NO + 4;                       // 77
    /** 整条 undo log 链的 record 总数（u64）。 */
    static final int LOG_RECORD_COUNT = LAST_PAGE_NO + 4;                    // 81
    /** 整条 undo log 链最近一条 record 的 undoNo（u64）。 */
    static final int LOG_LAST_UNDO_NO = LOG_RECORD_COUNT + 8;                // 89
    /** 提交序号 TransactionNo（u64，R 1.3）；ACTIVE 时为 0，commit 标 COMMITTED 时写入，恢复重建 history 用。 */
    static final int COMMIT_NO = LOG_LAST_UNDO_NO + 8;                       // 97
    /** 当前有效逻辑链头 undoNo（u64）；0 必须与 NULL pointer 成对，部分回滚会退回该值。 */
    static final int LOGICAL_LAST_UNDO_NO = COMMIT_NO + 8;                  // 105
    /** 当前有效逻辑链头 RollPointer（7B）；与 {@link #LOGICAL_LAST_UNDO_NO} 组成单个 15B after-image。 */
    static final int LOGICAL_HEAD_ROLL_POINTER = LOGICAL_LAST_UNDO_NO + 8;  // 113
    /** 持久 history 前驱 UPDATE undo first pageNo（u64）；无前驱为 FIL_NULL。 */
    static final int HISTORY_PREV_PAGE_NO = LOGICAL_HEAD_ROLL_POINTER + RollPointer.BYTES; // 120
    /** 持久 history 后继 UPDATE undo first pageNo（u64）；无后继为 FIL_NULL。 */
    static final int HISTORY_NEXT_PAGE_NO = HISTORY_PREV_PAGE_NO + 8;       // 128
    /** v3 undo log header 末尾。 */
    static final int LOG_HEADER_END = HISTORY_NEXT_PAGE_NO + 8;             // 136

    /** record area 起点；所有页一致，record 槽格式为 [len u16][payload]。 */
    static final int RECORD_AREA_START = LOG_HEADER_END;

    /** undo log 状态：ACTIVE（事务未提交，恢复期需回滚）。 */
    static final int STATE_ACTIVE = 0;
    /** undo log 状态：COMMITTED（事务已提交，恢复期跳过回滚，R 1.2）。 */
    static final int STATE_COMMITTED = 1;
    /** undo log 状态：CACHED（无事务 owner，page3 cached 栈持久拥有该单页 FSP segment）。 */
    static final int STATE_CACHED = 2;
    /** {@link #PAGE_FLAGS} bit0：first page 标志。 */
    static final int FLAG_FIRST_PAGE = 0x01;
    /** {@link #PAGE_FLAGS} 中格式版本的最低 bit。 */
    static final int FORMAT_VERSION_SHIFT = 1;
    /** {@link #PAGE_FLAGS} bits1..7：格式版本掩码。 */
    static final int FORMAT_VERSION_MASK = 0xFE;
    /** 当前带持久 history 链接的独立 INSERT/UPDATE log 页格式；v1/v2 不兼容且 fail-closed。 */
    static final int CURRENT_FORMAT_VERSION = 3;
    /** 当前版本写入每张 undo 页的 flags 基值；first 页另 OR {@link #FLAG_FIRST_PAGE}。 */
    static final int CURRENT_FORMAT_FLAGS = CURRENT_FORMAT_VERSION << FORMAT_VERSION_SHIFT;
}
