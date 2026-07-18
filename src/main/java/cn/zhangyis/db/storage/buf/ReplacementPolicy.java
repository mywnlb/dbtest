package cn.zhangyis.db.storage.buf;

/**
 * Buffer Pool 页面替换策略（Strategy）。所有方法由 BufferPool 在 list/meta 兼容锁下调用，实现无需自身线程安全。
 */
interface ReplacementPolicy {

    /** 记录一次对帧的访问/固定，更新最近度。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    void onAccess(BufferFrame frame);

    /** 记录帧成为驻留。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    void onInsert(BufferFrame frame);

    /** 记录帧被淘汰移除。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    void onRemove(BufferFrame frame);

    /**
     * 按淘汰优先序（LRU 在前）遍历当前驻留帧。调用方找到首个 fixCount==0 即 break，
     * 不得在迭代过程中调用 onRemove 改动内部结构（否则 ConcurrentModificationException）。
     *
     * @return {@code victimOrder} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    Iterable<BufferFrame> victimOrder();
}
