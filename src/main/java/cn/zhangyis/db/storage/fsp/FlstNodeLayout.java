package cn.zhangyis.db.storage.fsp;

/**
 * FLST 节点条内偏移：prev/next 各一个 FileAddress(12)。一个 node 由其起址（指向 PREV）定位。
 * XDES extent entry 的 prev/next 恰好就是一个 node（entryOffset+ExtentDescriptorLayout.PREV 为 node 起址）。
 */
final class FlstNodeLayout {

    private FlstNodeLayout() {
    }

    static final int PREV = 0;          // FileAddress 12
    static final int NEXT = PREV + 12;  // 12, FileAddress 12
    static final int SIZE = NEXT + 12;  // 24
}
