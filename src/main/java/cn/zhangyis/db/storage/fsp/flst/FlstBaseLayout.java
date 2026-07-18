package cn.zhangyis.db.storage.fsp.flst;

/**
 * FLST base（表头）条内偏移：length(long) + first(FileAddress) + last(FileAddress)（InnoDB FLST base）。
 */
public final class FlstBaseLayout {

    private FlstBaseLayout() {
    }

    /**
     * 持久结构布局常量；它定义 {@code FlstBaseLayout} 中 {@code LEN} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int LEN = 0;            // long 8
    /**
     * 持久结构布局常量；它定义 {@code FlstBaseLayout} 中 {@code FIRST} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int FIRST = LEN + 8;    // 8, FileAddress 12
    /**
     * 持久结构布局常量；它定义 {@code FlstBaseLayout} 中 {@code LAST} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int LAST = FIRST + 12;  // 20, FileAddress 12
    /**
     * 持久结构布局常量；它定义 {@code FlstBaseLayout} 中 {@code SIZE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int SIZE = LAST + 12;   // 32
}
