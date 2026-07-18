package cn.zhangyis.db.storage.buf;

/**
 * PageGuard 关闭时回调 BufferPool 释放帧的内部接口：在目标 frameMutex 下按是否写过 OR 置脏，并递减 fixCount。
 * 抽出此接口使 PageGuard 与具体 pool 解耦，便于单测注入假实现。
 */
interface FrameReleaser {

    /**
     * 标记一次活跃页写入。默认空实现保持旧测试替身和 lambda 的二元 release 契约兼容。
     *
     * @param frame 被当前 PageGuard 修改的 frame。
     */
    default void markWritePending(BufferFrame frame) {
    }

    /**
     * 释放一帧。由 PageGuard.close() 在已释放 page latch 之后调用。
     *
     * @param frame 被释放的帧。
     * @param wrote 本次持有期间是否写过页内容（用于 OR 置 dirty）。
     */
    void release(BufferFrame frame, boolean wrote);
}
