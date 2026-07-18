package cn.zhangyis.db.storage.flush.doublewrite;

/**
 * 双写物理文件的逻辑通道。文件身份本身就是恢复时的来源标识，避免把 flush 语义写入页帧。
 */
public enum DoublewriteChannelId {
    /** 面向 oldest dirty LSN 和 checkpoint 推进的 FlushList 文件。 */
    FLUSH_LIST,
    /** 面向 LRU 腾挪和 single-page flush 的 LRU 文件。 */
    LRU
}
