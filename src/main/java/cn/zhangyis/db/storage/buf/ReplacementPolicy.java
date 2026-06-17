package cn.zhangyis.db.storage.buf;

/**
 * Buffer Pool 页面替换策略（Strategy）。所有方法由 BufferPool 在 poolLock 下调用，实现无需自身线程安全。
 */
interface ReplacementPolicy {

    /** 记录一次对帧的访问/固定，更新最近度。 */
    void onAccess(BufferFrame frame);

    /** 记录帧成为驻留。 */
    void onInsert(BufferFrame frame);

    /** 记录帧被淘汰移除。 */
    void onRemove(BufferFrame frame);

    /**
     * 按淘汰优先序（LRU 在前）遍历当前驻留帧。调用方找到首个 fixCount==0 即 break，
     * 不得在迭代过程中调用 onRemove 改动内部结构（否则 ConcurrentModificationException）。
     */
    Iterable<BufferFrame> victimOrder();
}
