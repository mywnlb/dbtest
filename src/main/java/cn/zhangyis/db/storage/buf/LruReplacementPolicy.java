package cn.zhangyis.db.storage.buf;

import java.util.LinkedHashSet;

/**
 * LRU 替换策略：用访问序集合维护驻留帧，迭代序即 LRU→MRU。onAccess 通过 remove+add 把帧移到 MRU 尾。
 * 不自身加锁，依赖 BufferPool 的 poolLock 串行化。
 */
final class LruReplacementPolicy implements ReplacementPolicy {

    /** 访问序集合；头部为最久未访问（淘汰优先）。 */
    private final LinkedHashSet<BufferFrame> order = new LinkedHashSet<>();

    @Override
    public void onAccess(BufferFrame frame) {
        order.remove(frame);
        order.add(frame);
    }

    @Override
    public void onInsert(BufferFrame frame) {
        order.add(frame);
    }

    @Override
    public void onRemove(BufferFrame frame) {
        order.remove(frame);
    }

    @Override
    public Iterable<BufferFrame> victimOrder() {
        return order;
    }
}
