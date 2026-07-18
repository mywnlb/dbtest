package cn.zhangyis.db.storage.fsp.flst;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLayout;


/**
 * FLST 节点条内偏移：prev/next 各一个 FileAddress(12)。一个 node 由其起址（指向 PREV）定位。
 * XDES extent entry 的 prev/next 恰好就是一个 node（entryOffset+ExtentDescriptorLayout.PREV 为 node 起址）。
 */
public final class FlstNodeLayout {

    private FlstNodeLayout() {
    }

    /**
     * 持久结构布局常量；它定义 {@code FlstNodeLayout} 中 {@code PREV} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int PREV = 0;          // FileAddress 12
    /**
     * 持久结构布局常量；它定义 {@code FlstNodeLayout} 中 {@code NEXT} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int NEXT = PREV + 12;  // 12, FileAddress 12
    /**
     * 持久结构布局常量；它定义 {@code FlstNodeLayout} 中 {@code SIZE} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    public static final int SIZE = NEXT + 12;  // 24
}
