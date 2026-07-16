package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.List;

/**
 * 当前 active MTR 新建 LOB 页链的 RAII ownership guard。创建线程在 actual undo/row publish 前持有它；未调用
 * {@link #transferOwnership()} 就关闭会反序回收全部新页。该对象不允许跨线程或跨 MTR 生命周期使用。
 */
public final class LobWriteAllocation implements AutoCloseable {

    /** guard 生命周期；仅创建线程读写，不需要 monitor/原子变量。 */
    private enum State {
        OWNED,
        TRANSFERRED,
        COMPENSATED,
        COMPENSATION_FAILED
    }

    /** 执行分配的 storage facade；只用于同 MTR 补偿。 */
    private final LobStorage storage;

    /** 分配所属 MTR；ACTIVE 是合法补偿/转移的必要条件。 */
    private final MiniTransaction mtr;

    /** 页链所属 segment identity。 */
    private final SegmentRef segment;

    /** 按分配顺序冻结的新页集合；补偿时反序处理。 */
    private final List<PageId> allocatedPages;

    /** 可编码进 INSERT undo/clustered row 的 external envelope。 */
    private final ColumnValue.ExternalValue value;

    /** 单线程 owner；防止另一个线程在原线程发布 row 时并发补偿。 */
    private final Thread ownerThread;

    /** 当前权威 guard 状态，仅 owner thread 修改。 */
    private State state = State.OWNED;

    /** 首次补偿失败；重复 close 必须保留相同根因，不能重新释放部分已回收页。 */
    private LobAllocationStateException compensationFailure;

    LobWriteAllocation(LobStorage storage, MiniTransaction mtr, SegmentRef segment,
                       List<PageId> allocatedPages, ColumnValue.ExternalValue value) {
        if (storage == null || mtr == null || segment == null || allocatedPages == null
                || allocatedPages.isEmpty() || value == null) {
            throw new DatabaseValidationException("LOB write allocation fields must not be null or empty");
        }
        this.storage = storage;
        this.mtr = mtr;
        this.segment = segment;
        this.allocatedPages = List.copyOf(allocatedPages);
        this.value = value;
        this.ownerThread = Thread.currentThread();
    }

    /** 返回已经完整格式化、但 ownership 尚可能由 guard 补偿的 external value。 */
    public ColumnValue.ExternalValue value() {
        return value;
    }

    /**
     * 把页链 ownership 转交给即将在同一 MTR 发布的 undo/row。转移后 close 为 no-op；调用方仍必须保证 MTR commit
     * 失败时按上层 fail-stop/rollback 规则处理，而不能再次用本 guard 释放可能已经可见的引用。
     */
    public void transferOwnership() {
        requireOwnerThread();
        if (state == State.TRANSFERRED) {
            return;
        }
        if (state != State.OWNED || mtr.state() != MiniTransactionState.ACTIVE) {
            throw new LobAllocationStateException("cannot transfer LOB allocation in guard/MTR state "
                    + state + "/" + mtr.state());
        }
        state = State.TRANSFERRED;
    }

    /** 未转移时仅在原 active MTR 内反序补偿；终态、跨线程与既有补偿失败均 fail-closed。 */
    @Override
    public void close() {
        requireOwnerThread();
        if (state == State.TRANSFERRED || state == State.COMPENSATED) {
            return;
        }
        if (state == State.COMPENSATION_FAILED) {
            throw compensationFailure;
        }
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            compensationFailure = new LobAllocationStateException(
                    "cannot compensate LOB allocation after MTR left ACTIVE: " + mtr.state());
            state = State.COMPENSATION_FAILED;
            throw compensationFailure;
        }
        try {
            storage.compensateAllocation(mtr, segment, allocatedPages);
            state = State.COMPENSATED;
        } catch (LobAllocationStateException failure) {
            compensationFailure = failure;
            state = State.COMPENSATION_FAILED;
            throw failure;
        }
    }

    private void requireOwnerThread() {
        if (Thread.currentThread() != ownerThread) {
            throw new LobAllocationStateException("LOB allocation guard used by non-owner thread: owner="
                    + ownerThread.getName() + ", current=" + Thread.currentThread().getName());
        }
    }
}
