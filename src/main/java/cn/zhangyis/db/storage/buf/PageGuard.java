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

    /** 关闭回调，回到 pool 在 instanceLock 下 OR 脏并 unfix。 */
    private final FrameReleaser releaser;

    /** 被访问的帧。 */
    private final BufferFrame frame;

    /** 本句柄持有的 latch 模式。 */
    private final PageLatchMode mode;

    /** 已锁定、待 close 释放的 page latch（read 或 write lock）。 */
    private final Lock heldLatch;

    /** 持有期间是否写过；close 时 OR 进 frame.dirty。 */
    private boolean wrote;

    /** 幂等关闭标志。 */
    private boolean closed;

    /** 写监听；默认 NO_OP（非 MTR 路径零行为变化）。MTR 在 fix 后挂上 collector 以收集 redo。 */
    private PageWriteListener listener = PageWriteListener.NO_OP;

    PageGuard(FrameReleaser releaser, BufferFrame frame, PageLatchMode mode, Lock heldLatch) {
        this.releaser = releaser;
        this.frame = frame;
        this.mode = mode;
        this.heldLatch = heldLatch;
    }

    /** 当前页号。 */
    public PageId pageId() {
        ensureOpen();
        return frame.pageId;
    }

    /** 读 4 字节大端整数。S/X 均可。 */
    public int readInt(int offset) {
        ensureOpen();
        checkBounds(offset, Integer.BYTES);
        return frame.buffer.getInt(offset);
    }

    /** 读 length 字节副本。S/X 均可。 */
    public byte[] readBytes(int offset, int length) {
        ensureOpen();
        checkBounds(offset, length);
        byte[] dst = new byte[length];
        frame.buffer.get(offset, dst, 0, length);
        return dst;
    }

    /** 写 4 字节大端整数。要求 EXCLUSIVE。 */
    public void writeInt(int offset, int value) {
        requireExclusive();
        checkBounds(offset, Integer.BYTES);
        frame.buffer.putInt(offset, value);
        wrote = true;
        notifyWrite(offset, Integer.BYTES);
    }

    /** 读 8 字节大端长整数。S/X 均可。 */
    public long readLong(int offset) {
        ensureOpen();
        checkBounds(offset, Long.BYTES);
        return frame.buffer.getLong(offset);
    }

    /** 写 8 字节大端长整数。要求 EXCLUSIVE。 */
    public void writeLong(int offset, long value) {
        requireExclusive();
        checkBounds(offset, Long.BYTES);
        frame.buffer.putLong(offset, value);
        wrote = true;
        notifyWrite(offset, Long.BYTES);
    }

    /** 写字节。要求 EXCLUSIVE。 */
    public void writeBytes(int offset, byte[] src) {
        requireExclusive();
        if (src == null) {
            throw new DatabaseValidationException("write source must not be null");
        }
        checkBounds(offset, src.length);
        frame.buffer.put(offset, src, 0, src.length);
        wrote = true;
        notifyWrite(offset, src.length);
    }

    /** 显式标记本页将被置脏（用于不经 writeBytes 的修改场景）。要求 EXCLUSIVE。 */
    public void markDirty() {
        requireExclusive();
        wrote = true;
    }

    /** 挂载写监听（mtr fix 后调用；null 视为 NO_OP）。仅属主线程调用。 */
    public void attachWriteListener(PageWriteListener listener) {
        ensureOpen();
        this.listener = (listener == null) ? PageWriteListener.NO_OP : listener;
    }

    /** 写后回调：把该区间实际字节读回上报（listener 为 NO_OP 时跳过，零开销）。 */
    private void notifyWrite(int offset, int length) {
        if (listener != PageWriteListener.NO_OP) {
            byte[] b = new byte[length];
            frame.buffer.get(offset, b, 0, length);
            listener.onWrite(frame.pageId, offset, b);
        }
    }

    /** 释放：先放 page latch，再回调 pool 在 instanceLock 下 OR 脏并 unfix。幂等。 */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
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
