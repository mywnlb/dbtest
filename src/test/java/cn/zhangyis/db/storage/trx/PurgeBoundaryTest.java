package cn.zhangyis.db.storage.trx;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Purge 提交序低水位（设计 §7.7、§5.6）：{@code TransactionSystem.purgeLowWaterNo()} = 所有存活 ReadView 的
 * {@code min(lowLimitNo)}，无存活 ReadView 则 {@code nextTransactionNo}。RR 由 {@code release} 注销、
 * RC 由 {@code closeReadView} 注销后边界推进（评审 #1/#2）。
 */
class PurgeBoundaryTest {

    private TransactionSystem system;
    private TransactionManager txnMgr;
    private ReadViewManager rvm;

    private void setup() {
        system = new TransactionSystem();
        txnMgr = new TransactionManager(system);
        rvm = new ReadViewManager(system);
    }

    private Transaction begin(IsolationLevel level, boolean readOnly) {
        return txnMgr.begin(new TransactionOptions(level, readOnly, true));
    }

    /** 提交一个读写事务，推进 nextTransactionNo（commit 给已分配写 id 的事务分配 TransactionNo）。 */
    private void commitReadWrite() {
        Transaction rw = begin(IsolationLevel.REPEATABLE_READ, false);
        txnMgr.assignWriteId(rw);
        txnMgr.commit(rw);
    }

    @Test
    void noLiveReadViewBoundaryIsNextTransactionNo() {
        setup();
        assertEquals(1L, system.purgeLowWaterNo().value(), "无存活 ReadView：边界=nextTransactionNo(初值 1)");
        commitReadWrite(); // No=1, nextNo→2
        assertEquals(2L, system.purgeLowWaterNo().value(), "提交推进后仍无存活 ReadView：边界=nextTransactionNo");
    }

    @Test
    void boundaryIsMinLowLimitNoOverLiveReadViews() {
        setup();
        Transaction t1 = begin(IsolationLevel.REPEATABLE_READ, true);
        ReadView v1 = rvm.openReadView(t1);            // lowLimitNo = 1
        assertEquals(1L, v1.lowLimitNo());
        commitReadWrite();                              // nextNo → 2
        Transaction t2 = begin(IsolationLevel.REPEATABLE_READ, true);
        ReadView v2 = rvm.openReadView(t2);            // lowLimitNo = 2
        assertEquals(2L, v2.lowLimitNo());

        assertEquals(1L, system.purgeLowWaterNo().value(), "min(1,2)=1：最老快照挡住边界");
        rvm.release(t1);                                // 注销 v1
        assertEquals(2L, system.purgeLowWaterNo().value(), "release 最老 RR 快照后 min=2");
        rvm.release(t2);                                // 注销 v2
        assertEquals(2L, system.purgeLowWaterNo().value(), "无存活 → nextTransactionNo=2");
    }

    @Test
    void readCommittedViewReleasedByCloseAdvancesBoundary() {
        setup();
        commitReadWrite(); // nextNo → 2
        Transaction rc = begin(IsolationLevel.READ_COMMITTED, true);
        ReadView v = rvm.openReadView(rc);             // lowLimitNo = 2，注册
        assertEquals(2L, v.lowLimitNo());
        commitReadWrite(); // nextNo → 3
        assertEquals(2L, system.purgeLowWaterNo().value(), "RC 快照存活挡住边界于 2");
        rvm.closeReadView(v); // RC 语句末注销
        assertEquals(3L, system.purgeLowWaterNo().value(), "RC closeReadView 后边界推进到 nextTransactionNo=3");
    }
}
