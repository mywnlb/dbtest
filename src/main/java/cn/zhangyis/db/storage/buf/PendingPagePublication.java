package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一次 LOADING 页的发布前能力对象。它只允许认领一次不增加 fixCount 的 X guard：LOADING owner 原有的单个 fix
 * 会在普通 {@link BufferPool#getPage} 返回 guard 或 prefetch 收尾时释放，pending guard 只负责 latch、写监听和 dirty 发布。
 *
 * <p>并发边界：对象由单个 IO owner 创建，但 guard 可能被 MTR memo 延后到回调返回后关闭，因此 claimed、writePending、
 * closed 使用显式原子/volatile 可见性。页面仍由 page X latch 保护；Buffer Pool 只在回调完成后唤醒 LOADING 等待者。</p>
 */
public final class PendingPagePublication implements FrameReleaser {

    /** 创建本能力的唯一分片 owner。 */
    private final BufferPoolInstance owner;
    /** 仍处于 LOADING 的目标 frame；不得泄漏给包外调用方。 */
    private final BufferFrame frame;
    /** 单次认领守卫，防两个协作者分别把同一 loader fix 挂进不同 MTR。 */
    private final AtomicBoolean claimed = new AtomicBoolean();
    /** 第一次页写原语执行前发布；供 readAndPublish 决定 CLEAN 或 DIRTY_PENDING。 */
    private volatile boolean writePending;
    /** pending guard 是否已关闭；prefetch 自有 MTR 会在真正 publish 前关闭。 */
    private volatile boolean closed;

    PendingPagePublication(BufferPoolInstance owner, BufferFrame frame) {
        if (owner == null || frame == null) {
            throw new DatabaseValidationException("pending page publication owner/frame must not be null");
        }
        this.owner = owner;
        this.frame = frame;
    }

    /**
     * @return 本次尚未发布页的稳定物理 identity
     */
    public PageId pageId() {
        return frame.pageId;
    }

    /**
     * 读取刚由 PageStore 填充的 FIL page type。调用发生时 frame 仍由唯一 LOADING owner 持有，普通 fixer 尚不可见，
     * 因而不需要认领 X guard；该方法只用于拦截器在产生任何写副作用前过滤无关页类型。
     *
     * @return 磁盘页头声明的持久页类型
     * @throws DatabaseValidationException page type code 未注册时抛出，调用方不得发布该损坏页
     */
    public PageType loadedPageType() {
        return PageType.fromCode(frame.buffer.getInt(PageEnvelopeLayout.PAGE_TYPE));
    }

    /**
     * 认领独占页 guard。等待使用 Buffer Pool 配置的 load timeout，正常情况下 LOADING 页没有任何普通 guard，
     * 因而应立即成功；超时表示内部生命周期不变量被破坏。生产写入必须优先通过
     * {@link cn.zhangyis.db.storage.mtr.MiniTransaction#adoptPendingPage(PendingPagePublication)} 接管，使 guard、redo、
     * pageLSN 和失败释放归同一 MTR；直接调用只供 Buffer Pool 协议测试或保证只读后立即关闭的包级适配器。
     *
     * @return 不拥有额外 fix、但拥有 page X latch 的受控 guard；必须关闭或交给 MTR memo
     * @throws DatabaseValidationException 同一 publication 被重复认领时抛出
     * @throws BufferPoolLoadTimeoutException X latch 在有界时间内无法取得或线程被中断时抛出
     */
    public PageGuard claimExclusive() {
        if (!claimed.compareAndSet(false, true)) {
            throw new DatabaseValidationException("pending page publication already claimed: " + pageId());
        }
        try {
            return owner.claimPendingPage(this, frame);
        } catch (RuntimeException failure) {
            claimed.set(false);
            throw failure;
        }
    }

    /** PageGuard 第一次写前回调；这里只发布 pending 事实，不在 LOADING 阶段把 frame 加入 flush list。 */
    @Override
    public void markWritePending(BufferFrame changedFrame) {
        requireSameFrame(changedFrame);
        writePending = true;
    }

    /**
     * 关闭 pending guard：释放 page latch 后由 PageGuard 调用。若页面仍 LOADING，dirty 发布延迟到
     * readAndPublish；若已发布为 DIRTY_PENDING，则由 owner 用已盖 pageLSN 建立 flush-list 边界。
     */
    @Override
    public void release(BufferFrame changedFrame, boolean wrote) {
        requireSameFrame(changedFrame);
        if (closed) {
            throw new DatabaseValidationException("pending page publication released twice: " + pageId());
        }
        closed = true;
        owner.releasePendingPage(frame, wrote);
    }

    boolean claimed() {
        return claimed.get();
    }

    boolean writePending() {
        return writePending;
    }

    boolean closed() {
        return closed;
    }

    private void requireSameFrame(BufferFrame changedFrame) {
        if (changedFrame != frame) {
            throw new DatabaseValidationException("pending publication frame identity mismatch");
        }
    }
}
