package cn.zhangyis.db.storage.record.page;

/**
 * 页内插入方向（innodb-record-design §7 PAGE_DIRECTION）。落盘 u16 code，配合 PAGE_N_DIRECTION 让 B+Tree
 * 识别顺序插入并优化 split 点。本片只编解码该字段，不实现方向驱动的 split 优化（归 R4/B+Tree）。
 */
public enum IndexPageDirection {
    /** 无明显方向（初始或方向翻转）。 */
    NO_DIRECTION(0),
    /** 连续向较小 key 方向插入。 */
    LEFT(1),
    /** 连续向较大 key 方向插入。 */
    RIGHT(2);

    private final int code;

    IndexPageDirection(int code) {
        this.code = code;
    }

    /** 落盘 code。 */
    public int code() {
        return code;
    }

    /** 由 code 还原；未知 code 视为 page header 损坏。 */
    public static IndexPageDirection fromCode(int code) {
        for (IndexPageDirection d : values()) {
            if (d.code == code) {
                return d;
            }
        }
        throw new PageDirectoryCorruptedException("unknown page direction code: " + code);
    }
}
