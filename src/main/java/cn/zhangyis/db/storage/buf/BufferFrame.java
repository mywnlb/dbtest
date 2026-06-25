package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Buffer Pool 一帧，承载单个驻留页的内容与状态。内部数据结构（包内可见），字段直接由同包协作者访问。
 *
 * <p>并发归属（AGENTS.md 要求逐字段写明）：
 * <ul>
 *   <li>pageId / dirty / fixCount / dirty LSN / dirtyVersion：由 BufferPool 的 poolLock 保护。</li>
 *   <li>data / buffer 内容：由 pageLatch(S/X) 保护；仅在 fixCount==0（无活跃 PageGuard）时由 pool 在 poolLock 下读写（载入/写回/清零）。</li>
 *   <li>pageLatch：协调同一驻留页活跃 fixer 的读写并发；淘汰/flush 只作用于 fixCount==0 的帧，不取此闩。</li>
 * </ul>
 */
final class BufferFrame {

    /** 当前驻留页号；null 表示空闲帧。由 poolLock 保护。 */
    PageId pageId;

    /** 页内容字节，帧创建时按 pageSize 分配一次、跨驻留复用。内容由 pageLatch 保护。 */
    final byte[] data;

    /** 绝对访问视图，wrap 同一 data 数组；PageGuard 用绝对 get/put（不动 position）读写，故可并发只读。 */
    final ByteBuffer buffer;

    /** 是否含未落盘修改。由 poolLock 保护：PageGuard.close 按是否写过 OR 置位，flush/写回后清零，淘汰时读取。 */
    boolean dirty;

    /**
     * 帧生命周期 / IO 状态（§5.7）。由 poolLock 保护，经 {@link FrameStateMachine} 转换，取代隐式的"空闲/载入中/刷盘中"布尔。
     * 不变量：{@code dirty ⟺ state ∈ {DIRTY, FLUSHING}}；新帧初始 FREE，载入期 LOADING，刷盘期 FLUSHING（仍 dirty）。
     */
    BufferFrameState state = BufferFrameState.FREE;

    /** 首次变脏的 LSN；只有 dirty=true 时有效，checkpoint 不能越过该值。由 poolLock 保护。 */
    Lsn oldestModificationLsn;

    /** 最近一次修改后的页 LSN；只有 dirty=true 时有效，flush 写 data file 前必须等 redo durable 覆盖该值。由 poolLock 保护。 */
    Lsn newestModificationLsn;

    /** 脏版本号；每次写关闭时递增，flush snapshot 用它识别 snapshot 后再次修改。由 poolLock 保护。 */
    long dirtyVersion;

    /** 固定计数，>0 不可被淘汰。由 poolLock 保护。 */
    int fixCount;

    /** 页内容 S/X 闩。 */
    final ReentrantReadWriteLock pageLatch = new ReentrantReadWriteLock();

    BufferFrame(PageSize pageSize) {
        this.data = new byte[pageSize.bytes()];
        this.buffer = ByteBuffer.wrap(data);
    }
}
