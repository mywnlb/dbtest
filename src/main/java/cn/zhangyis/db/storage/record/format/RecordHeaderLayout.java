package cn.zhangyis.db.storage.record.format;

/**
 * 记录头偏移（简化定长头，前向布局，非 InnoDB 二进制兼容）。innodb-record-design §5.2。
 */
final class RecordHeaderLayout {

    private RecordHeaderLayout() {
    }

    /** flags：bit0 deleted、bit1 minRec、bit2-3 recordType code。
     *
     * 持久结构布局常量；它定义 {@code RecordHeaderLayout} 中 {@code FLAGS} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    static final int FLAGS = 0;            // 1 byte
    /**
     * 持久结构布局常量；它定义 {@code RecordHeaderLayout} 中 {@code HEAP_NO} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    static final int HEAP_NO = 1;          // 2 bytes (u16)
    /**
     * 持久结构布局常量；它定义 {@code RecordHeaderLayout} 中 {@code N_OWNED} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    static final int N_OWNED = 3;          // 1 byte
    /**
     * 持久结构布局常量；它定义 {@code RecordHeaderLayout} 中 {@code NEXT_RECORD_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    static final int NEXT_RECORD_OFFSET = 4; // 2 bytes (u16)
    /**
     * 持久结构布局常量；它定义 {@code RecordHeaderLayout} 中 {@code RECORD_LENGTH} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    static final int RECORD_LENGTH = 6;    // 2 bytes (u16)
    /**
     * 持久结构布局常量；它定义 {@code RecordHeaderLayout} 中 {@code SIZE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    static final int SIZE = 8;
}
