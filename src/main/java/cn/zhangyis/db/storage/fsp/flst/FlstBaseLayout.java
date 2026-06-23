package cn.zhangyis.db.storage.fsp.flst;

/**
 * FLST base（表头）条内偏移：length(long) + first(FileAddress) + last(FileAddress)（InnoDB FLST base）。
 */
public final class FlstBaseLayout {

    private FlstBaseLayout() {
    }

    public static final int LEN = 0;            // long 8
    public static final int FIRST = LEN + 8;    // 8, FileAddress 12
    public static final int LAST = FIRST + 12;  // 20, FileAddress 12
    public static final int SIZE = LAST + 12;   // 32
}
