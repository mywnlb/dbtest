package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.undo.InsertedLobOwnership;

import java.util.List;
import java.util.function.Function;

/**
 * 单一业务 MTR 拥有的 prepared undo append guard。prepare 已固定 slot owner、undo root 与 external payload 页，
 * 但尚未写入 record slot；调用者必须恰好一次发布真实 ownership。未发布即关闭表示跨过无 content-undo 的物理边界，
 * 因而释放资源后抛 fatal，禁止把半准备状态当成普通可重试失败。
 */
public final class PreparedUndoAppend implements AutoCloseable {

    /** 创建线程；MTR memo 与显式 latch 不允许跨线程转移。 */
    private final Thread ownerThread;
    /** 所属业务 MTR；append/close 前必须仍为 ACTIVE。 */
    private final MiniTransaction mtr;
    /** manager 提供的 actual plan 校验、物理 append 与事务 context 发布闭包。 */
    private final Function<List<InsertedLobOwnership>, RollPointer> appender;
    /** 反序关闭 reservation/slot/reuse lease 的幂等闭包。 */
    private final Runnable releaser;
    /** 是否已经成功写入 actual undo 并发布逻辑头。 */
    private boolean published;
    /** guard 是否已经关闭。 */
    private boolean closed;

    PreparedUndoAppend(MiniTransaction mtr,
                       Function<List<InsertedLobOwnership>, RollPointer> appender,
                       Runnable releaser) {
        if (mtr == null || appender == null || releaser == null) {
            throw new DatabaseValidationException("prepared undo append fields must not be null");
        }
        this.ownerThread = Thread.currentThread();
        this.mtr = mtr;
        this.appender = appender;
        this.releaser = releaser;
    }

    /** 校验并发布真实 INSERT undo；shape 校验失败不写页，调用方仍可提供正确 ownership 重试一次。 */
    public RollPointer appendActual(List<InsertedLobOwnership> actualOwnerships) {
        requireOpen("append actual undo");
        if (published) {
            throw new UndoWriteStalePlanException("prepared undo append was already published");
        }
        RollPointer pointer = appender.apply(actualOwnerships);
        published = true;
        return pointer;
    }

    /**
     * 结束 guard 并释放非页资源。published 路径正常返回；未 published 路径始终 fail-stop，因为 slot/page owner 或
     * prepared 页可能已经写入 buffer，MTR rollback 不能撤销这些 page content。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        requireOwner();
        closed = true;
        RuntimeException releaseFailure = null;
        try {
            releaser.run();
        } catch (RuntimeException error) {
            releaseFailure = error;
        }
        if (!published) {
            throw new UndoWriteFatalException("prepared undo append closed before actual undo publication",
                    releaseFailure);
        }
        if (releaseFailure != null) {
            throw new UndoWriteFatalException("prepared undo append resource release failed", releaseFailure);
        }
    }

    private void requireOpen(String operation) {
        requireOwner();
        if (closed) {
            throw new UndoWriteStalePlanException(operation + " on closed prepared undo append");
        }
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            throw new UndoWriteFatalException(operation + " requires ACTIVE MTR: " + mtr.state());
        }
    }

    private void requireOwner() {
        if (Thread.currentThread() != ownerThread) {
            throw new UndoWriteFatalException("prepared undo append used by a non-owner thread");
        }
    }
}
