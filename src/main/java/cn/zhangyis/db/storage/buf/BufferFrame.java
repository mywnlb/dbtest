package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Buffer Pool 一帧，承载单个驻留页的内容与状态。内部数据结构（包内可见），字段直接由同包协作者访问。
 *
 * <p>并发归属（AGENTS.md 要求逐字段写明）：
 * <ul>
 *   <li>pageId / dirty / fixCount / dirty LSN / dirtyVersion：13.1c 起由本帧 {@link #frameMutex} 保护。</li>
 *   <li>data / buffer 内容：由 pageLatch(S/X) 保护；仅在 fixCount==0（无活跃 PageGuard）或 LOADING owner 独占时由 instance 写入/复制。</li>
 *   <li>pageLatch：协调同一驻留页活跃 fixer 的读写并发；淘汰/flush 只作用于 fixCount==0 的帧，不取此闩。</li>
 * </ul>
 */
final class BufferFrame {

    /**
     * 保护单个 frame 的元数据。13.1c 后 page hash 只负责 pageId→frame 的映射存在性，frame 当前绑定页、
     * 生命周期状态、fix 计数、dirty/LSN 和载入 future 均由本锁串行，避免无关 frame 互相阻塞。
     */
    final ReentrantLock frameMutex = new ReentrantLock();

    /** 当前驻留页号；null 表示空闲帧。由 frameMutex 保护。 */
    PageId pageId;

    /**
     * 当前 frame 绑定 {@link #pageId} 时采用的表空间生命周期版本。由 frameMutex 保护；FREE 帧为 null。
     * Buffer Pool 命中页时必须与 {@link SpaceLifecycleClock} 当前版本一致，否则该帧属于 truncate/drop/discard 前的
     * 旧代际，不能返回给普通查询路径。
     */
    TablespaceVersion spaceVersion;

    /** 页内容字节，帧创建时按 pageSize 分配一次、跨驻留复用。内容由 pageLatch 保护。 */
    final byte[] data;

    /** 绝对访问视图，wrap 同一 data 数组；PageGuard 用绝对 get/put（不动 position）读写，故可并发只读。 */
    final ByteBuffer buffer;

    /** 是否含未落盘修改。由 frameMutex 保护：PageGuard.close 按是否写过 OR 置位，flush 完成后清零，淘汰时读取。 */
    boolean dirty;

    /**
     * 帧生命周期 / IO 状态（§5.7）。由 frameMutex 保护，经 {@link FrameStateMachine} 转换，取代隐式的"空闲/载入中/刷盘中"布尔。
     * 不变量：{@code dirty ⟺ state ∈ {DIRTY, FLUSHING}}；新帧初始 FREE，载入期 LOADING，刷盘期 FLUSHING（仍 dirty）。
     */
    BufferFrameState state = BufferFrameState.FREE;

    /** 首次变脏的 LSN；只有 dirty=true 时有效，checkpoint 不能越过该值。由 frameMutex 保护。 */
    Lsn oldestModificationLsn;

    /** 最近一次修改后的页 LSN；只有 dirty=true 时有效，flush 写 data file 前必须等 redo durable 覆盖该值。由 frameMutex 保护。 */
    Lsn newestModificationLsn;

    /** 脏版本号；每次写关闭时递增，flush snapshot 用它识别 snapshot 后再次修改。由 frameMutex 保护。 */
    long dirtyVersion;

    /** 固定计数，>0 不可被淘汰。由 frameMutex 保护。 */
    int fixCount;

    /**
     * 载入完成信号；仅在 state==LOADING 时非空，发布 CLEAN / 回收占位后置回 null。由 frameMutex 保护（赋值/读取）。
     * 命中 LOADING 帧的等待者在 pageHashLock+frameMutex 内取得本引用后出锁有界等待，避免持锁阻塞。
     */
    PageLoadFuture loadFuture;

    /** 页内容 S/X 闩。SHARED_EXCLUSIVE 也走它的 readLock（故与 S 并发、与 X 互斥）。 */
    final ReentrantReadWriteLock pageLatch = new ReentrantReadWriteLock();

    /**
     * SHARED_EXCLUSIVE（SIX）写意向闩：SX 在持 {@link #pageLatch} readLock 之外再持它，保证同一驻留页同一时刻
     * 只允许一个写意向者（SX 排斥 SX）。S 不取它（故 S 与 SX 并发）；X 不取它（X 由 writeLock 与 SX 的 readLock 互斥）。
     * 仅在 SX 授予时短暂持有、随 PageGuard.close 释放；持有期间该页 fixCount&gt;0 故帧不会被淘汰复用。
     */
    final ReentrantLock pageIntentLatch = new ReentrantLock();

    BufferFrame(PageSize pageSize) {
        this.data = new byte[pageSize.bytes()];
        this.buffer = ByteBuffer.wrap(data);
    }
}
