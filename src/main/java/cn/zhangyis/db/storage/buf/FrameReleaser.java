package cn.zhangyis.db.storage.buf;

/**
 * PageGuard 关闭时回调 BufferPool 释放帧的内部接口：在 poolLock 下按是否写过 OR 置脏，并递减 fixCount。
 * 抽出此接口使 PageGuard 与具体 pool 解耦，便于单测注入假实现。
 */
interface FrameReleaser {

    /**
     * 释放一帧。由 PageGuard.close() 在已释放 page latch 之后调用。
     *
     * @param frame 被释放的帧。
     * @param wrote 本次持有期间是否写过页内容（用于 OR 置 dirty）。
     */
    void release(BufferFrame frame, boolean wrote);
}
