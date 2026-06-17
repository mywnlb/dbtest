package cn.zhangyis.db.storage.record.format;

/**
 * 物理记录类型（innodb-record-design §5.2）。code 落盘（记录头 2 bit），取值不可改。
 */
public enum RecordType {
    /** 普通用户记录。 */
    CONVENTIONAL(0),
    /** 非叶子页的 node pointer 记录（指向子页）。 */
    NODE_POINTER(1),
    /** 页内最小边界记录。 */
    INFIMUM(2),
    /** 页内最大边界记录。 */
    SUPREMUM(3);

    private final int code;

    RecordType(int code) {
        this.code = code;
    }

    /** 落盘 code（0..3）。 */
    public int code() {
        return code;
    }

    /** 由 code 还原；未知 code 视为记录头损坏。 */
    public static RecordType fromCode(int code) {
        for (RecordType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new RecordFormatException("unknown record type code: " + code);
    }
}
