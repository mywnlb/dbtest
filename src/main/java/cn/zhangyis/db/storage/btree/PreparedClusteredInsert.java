package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservation;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.mtr.MtrLatchOrderScope;
import cn.zhangyis.db.storage.record.format.LogicalRecord;

/**
 * 单一 MTR 拥有的聚簇 INSERT prepare guard。构造时已经固定 index X 路径、唯一性结果与最坏 split 预留，
 * 但没有写入 placeholder row；调用者完成 LOB/undo 后以实际记录和 roll pointer 恰好发布一次。
 */
public final class PreparedClusteredInsert implements AutoCloseable {

    /** 实际 row 发布闭包；路径、leaf 和 split 预留仍由创建它的 B+Tree service 封装。 */
    @FunctionalInterface
    interface Publisher {
        BTreeInsertResult publish(LogicalRecord actualRecord, RollPointer rollPointer);
    }

    /** MTR memo 不允许跨线程转移，显式记录 owner 用于 fail-fast。 */
    private final Thread ownerThread;
    /** 所属业务 MTR。 */
    private final MiniTransaction mtr;
    /** 保持 index→FSP/LOB 局部逆序许可的无环证明 scope。 */
    private final MtrLatchOrderScope latchOrderScope;
    /** split 最坏页数预留；leaf 可直接容纳时为空。 */
    private final SpaceReservation reservation;
    /** 实际发布动作。 */
    private final Publisher publisher;
    /** 是否已成功发布 row。 */
    private boolean published;
    /** guard 是否已关闭。 */
    private boolean closed;

    PreparedClusteredInsert(MiniTransaction mtr, MtrLatchOrderScope latchOrderScope,
                            SpaceReservation reservation, Publisher publisher) {
        if (mtr == null || latchOrderScope == null || publisher == null) {
            throw new DatabaseValidationException("prepared clustered insert fields must not be null");
        }
        this.ownerThread = Thread.currentThread();
        this.mtr = mtr;
        this.latchOrderScope = latchOrderScope;
        this.reservation = reservation;
        this.publisher = publisher;
    }

    /** 用真实 external values 与真实 undo pointer 发布聚簇行；成功后关闭 split reservation 与越序 scope。 */
    public BTreeInsertResult publish(LogicalRecord actualRecord, RollPointer rollPointer) {
        requireOpen("publish clustered row");
        if (actualRecord == null || rollPointer == null) {
            throw new DatabaseValidationException("prepared clustered publish record/pointer must not be null");
        }
        BTreeInsertResult result = publisher.publish(actualRecord, rollPointer);
        published = true;
        releaseResources();
        return result;
    }

    /** published 后幂等结束；未发布关闭表示 prepared 物理边界失败，必须 fail-stop。 */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        requireOwner();
        closed = true;
        RuntimeException releaseFailure = releaseResources();
        if (!published) {
            throw new PreparedInsertStateException("prepared clustered insert closed before row publication",
                    releaseFailure);
        }
        if (releaseFailure != null) {
            throw new PreparedInsertStateException("prepared clustered insert resource release failed",
                    releaseFailure);
        }
    }

    private void requireOpen(String operation) {
        requireOwner();
        if (closed || published) {
            throw new PreparedInsertStateException(operation + " on closed/published prepared insert");
        }
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            throw new PreparedInsertStateException(operation + " requires ACTIVE MTR: " + mtr.state());
        }
    }

    private void requireOwner() {
        if (Thread.currentThread() != ownerThread) {
            throw new PreparedInsertStateException("prepared clustered insert used by a non-owner thread");
        }
    }

    /** 先释放 reservation、再退出越序 scope；返回首个异常并聚合 suppressed。 */
    private RuntimeException releaseResources() {
        RuntimeException first = null;
        if (reservation != null) {
            try {
                reservation.close();
            } catch (RuntimeException error) {
                first = error;
            }
        }
        try {
            latchOrderScope.close();
        } catch (RuntimeException error) {
            if (first == null) {
                first = error;
            } else {
                first.addSuppressed(error);
            }
        }
        return first;
    }
}
