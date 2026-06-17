package cn.zhangyis.db.storage.fsp;

/**
 * FLST base（表头）条内偏移：length(long) + first(FileAddress) + last(FileAddress)（InnoDB FLST base）。
 */
final class FlstBaseLayout {

    private FlstBaseLayout() {
    }

    static final int LEN = 0;            // long 8
    static final int FIRST = LEN + 8;    // 8, FileAddress 12
    static final int LAST = FIRST + 12;  // 20, FileAddress 12
    static final int SIZE = LAST + 12;   // 32
}
