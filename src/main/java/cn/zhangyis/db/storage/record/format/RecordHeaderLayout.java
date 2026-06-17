package cn.zhangyis.db.storage.record.format;

/**
 * 记录头偏移（简化定长头，前向布局，非 InnoDB 二进制兼容）。innodb-record-design §5.2。
 */
final class RecordHeaderLayout {

    private RecordHeaderLayout() {
    }

    /** flags：bit0 deleted、bit1 minRec、bit2-3 recordType code。 */
    static final int FLAGS = 0;            // 1 byte
    static final int HEAP_NO = 1;          // 2 bytes (u16)
    static final int N_OWNED = 3;          // 1 byte
    static final int NEXT_RECORD_OFFSET = 4; // 2 bytes (u16)
    static final int RECORD_LENGTH = 6;    // 2 bytes (u16)
    static final int SIZE = 8;
}
