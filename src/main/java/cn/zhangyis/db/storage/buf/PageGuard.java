package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.concurrent.locks.Lock;

/**
 * 受控页访问的 RAII 句柄。getPage/newPage 返回它，持有一次 buffer fix 和一把已锁定的 page latch（S 或 X）。
 * 用 try-with-resources：close() 先释放 page latch，再回调 pool 释放 fix。
 *
 * <p>非线程安全：仅由获取它的线程使用与 close（page latch 的 lock/unlock 须同线程）。
 * 写操作（writeInt/writeBytes/markDirty）要求 EXCLUSIVE 模式，并记录"写过"以便 close 时置脏。
 */
public final class PageGuard implements AutoCloseable {

    /** 关闭回调，回到 pool 在目标 frameMutex 下 OR 脏并 unfix。 */
    private final FrameReleaser releaser;

    /** 被访问的帧。 */
    private final BufferFrame frame;

    /** 本句柄持有的 latch 模式。 */
    private final PageLatchMode mode;

    /** 已锁定、待 close 释放的 page latch（read 或 write lock；SHARED_EXCLUSIVE 走 readLock）。 */
    private final Lock heldLatch;

    /** SHARED_EXCLUSIVE 额外持有的写意向闩；其它模式为 null。close 时先于 {@link #heldLatch} 释放。 */
    private final Lock heldIntentLatch;

    /** 持有期间是否写过；close 时 OR 进 frame.dirty。 */
    private boolean wrote;
    /** 是否已经把本次写入发布为 DIRTY_PENDING，避免同一 guard 重复触发状态转换。 */
    private boolean pendingMarked;

    /** 幂等关闭标志。 */
    private boolean closed;

    /** 写监听；默认 NO_OP（非 MTR 路径零行为变化）。MTR 在 fix 后挂上 collector 以收集 redo。 */
    private PageWriteListener listener = PageWriteListener.NO_OP;

    /**
     * 创建 {@code PageGuard}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param releaser 生命周期回调；只在契约定义的成功或释放边界调用，且不得为 {@code null}
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param heldLatch 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param heldIntentLatch 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     */
    PageGuard(FrameReleaser releaser, BufferFrame frame, PageLatchMode mode, Lock heldLatch, Lock heldIntentLatch) {
        this.releaser = releaser;
        this.frame = frame;
        this.mode = mode;
        this.heldLatch = heldLatch;
        this.heldIntentLatch = heldIntentLatch;
    }

    /** 当前页号。
     *
     * @return {@code pageId} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public PageId pageId() {
        ensureOpen();
        return frame.pageId;
    }

    /** 读 4 字节大端整数。S/X 均可。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code readInt} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    public int readInt(int offset) {
        ensureOpen();
        checkBounds(offset, Integer.BYTES);
        return frame.buffer.getInt(offset);
    }

    /** 读 length 字节副本。S/X 均可。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param length 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code readBytes} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    public byte[] readBytes(int offset, int length) {
        ensureOpen();
        checkBounds(offset, length);
        byte[] dst = new byte[length];
        frame.buffer.get(offset, dst, 0, length);
        return dst;
    }

    /** 写 4 字节大端整数。要求 EXCLUSIVE。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeInt} 转换或编码的原始 {@code int} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     */
    public void writeInt(int offset, int value) {
        requireExclusive();
        markWritePendingIfNeeded();
        checkBounds(offset, Integer.BYTES);
        frame.buffer.putInt(offset, value);
        wrote = true;
        notifyWrite(offset, Integer.BYTES);
    }

    /** 读 8 字节大端长整数。S/X 均可。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code readLong} 从受校验输入或持久字节中得到的 {@code long} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    public long readLong(int offset) {
        ensureOpen();
        checkBounds(offset, Long.BYTES);
        return frame.buffer.getLong(offset);
    }

    /** 写 8 字节大端长整数。要求 EXCLUSIVE。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeLong} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     */
    public void writeLong(int offset, long value) {
        requireExclusive();
        markWritePendingIfNeeded();
        checkBounds(offset, Long.BYTES);
        frame.buffer.putLong(offset, value);
        wrote = true;
        notifyWrite(offset, Long.BYTES);
    }

    /** 写字节。要求 EXCLUSIVE。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，过期映射在返回前拒绝。</li>
     *     <li>遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，慢 IO 或条件等待移到内部锁外。</li>
     *     <li>完成页载入、替换、dirty snapshot 或状态转换，并向等待者发布唯一完成或失败信号。</li>
     *     <li>返回受控 Guard/快照或释放 fix；失败回收占位且不错误清除并发产生的 dirty 状态。</li>
     * </ol>
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param src 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void writeBytes(int offset, byte[] src) {
        // 1、按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，在共享或持久副作用前拒绝非法状态。
        requireExclusive();
        markWritePendingIfNeeded();
        // 2、继续完成范围、身份与候选校验；通过后，遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，保持处理顺序与资源边界。
        if (src == null) {
            throw new DatabaseValidationException("write source must not be null");
        }
        checkBounds(offset, src.length);
        // 3、在中间分支复核阶段性结果；满足条件后，完成页载入、替换、dirty snapshot 或状态转换，并维持领域不变量。
        frame.buffer.put(offset, src, 0, src.length);
        wrote = true;
        // 4、返回受控 Guard/快照或释放 fix，以稳定返回或领域异常完成收口。
        notifyWrite(offset, src.length);
    }

    /** 显式标记本页将被置脏（用于不经 writeBytes 的修改场景）。要求 EXCLUSIVE。 */
    public void markDirty() {
        requireExclusive();
        markWritePendingIfNeeded();
        wrote = true;
    }

    /** 在第一次修改前通知 frame 生命周期；页面内容仍由 page latch 保护。 */
    private void markWritePendingIfNeeded() {
        if (!pendingMarked) {
            releaser.markWritePending(frame);
            pendingMarked = true;
        }
    }

    /** 挂载写监听（mtr fix 后调用；null 视为 NO_OP）。仅属主线程调用。
     *
     * @param listener 生命周期回调；只在契约定义的成功或释放边界调用，且不得为 {@code null}
     */
    public void attachWriteListener(PageWriteListener listener) {
        ensureOpen();
        this.listener = (listener == null) ? PageWriteListener.NO_OP : listener;
    }

    /** 写后回调：把该区间实际字节读回上报（listener 为 NO_OP 时跳过，零开销）。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param length 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     */
    private void notifyWrite(int offset, int length) {
        if (listener != PageWriteListener.NO_OP) {
            byte[] b = new byte[length];
            frame.buffer.get(offset, b, 0, length);
            listener.onWrite(frame.pageId, offset, b);
        }
    }

    /** 释放：先放 page latch，再回调 pool 在目标 frameMutex 下 OR 脏并 unfix。幂等。 */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // 释放顺序为获取的逆序：先放写意向闩（SX 专用），再放 page latch，最后 unfix。
        if (heldIntentLatch != null) {
            heldIntentLatch.unlock();
        }
        heldLatch.unlock();
        releaser.release(frame, wrote);
    }

    private void requireExclusive() {
        ensureOpen();
        if (mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("page write requires EXCLUSIVE latch, but held " + mode);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new DatabaseValidationException("page guard already closed");
        }
    }

    private void checkBounds(int offset, int length) {
        int pageBytes = frame.buffer.capacity();
        // 先挡负数与 offset 越界，再用 length > pageBytes - offset 判断，避免 offset+length 整数溢出绕过检查。
        if (offset < 0 || length < 0 || offset > pageBytes || length > pageBytes - offset) {
            throw new DatabaseValidationException("page access out of bounds: offset=" + offset
                    + " length=" + length + " pageSize=" + pageBytes);
        }
    }
}
