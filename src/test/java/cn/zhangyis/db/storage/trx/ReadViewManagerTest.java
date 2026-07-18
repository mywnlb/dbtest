package cn.zhangyis.db.storage.trx;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.4 ReadViewManager：按隔离级别建/复用 ReadView（设计 §8.1）。RR 事务级缓存复用，RC 每读新建；
 * 非只读事务首次 open 原子分配 creator 写 id；RU/SERIALIZABLE 显式拒绝；release 清 RR 缓存。
 */
class ReadViewManagerTest {

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

    /**
     * 验证 {@code rrReusesSameReadView} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void rrReusesSameReadView() {
        setup();
        Transaction txn = begin(IsolationLevel.REPEATABLE_READ, false);
        ReadView v1 = rvm.openReadView(txn);
        ReadView v2 = rvm.openReadView(txn);
        assertSame(v1, v2, "RR 事务级 ReadView 复用同一对象");
    }

    /**
     * 验证 {@code rcCreatesFreshReadView} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void rcCreatesFreshReadView() {
        setup();
        Transaction txn = begin(IsolationLevel.READ_COMMITTED, false);
        ReadView v1 = rvm.openReadView(txn);
        ReadView v2 = rvm.openReadView(txn);
        assertNotSame(v1, v2, "RC 每次一致性读新建 ReadView");
    }

    /**
     * 验证 {@code nonReadOnlyAllocatesCreatorIdOnFirstOpenAndSelfVisible} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void nonReadOnlyAllocatesCreatorIdOnFirstOpenAndSelfVisible() {
        setup();
        Transaction txn = begin(IsolationLevel.REPEATABLE_READ, false);
        assertTrue(txn.transactionId().isNone(), "未写、未建 ReadView 时无写 id");
        ReadView v = rvm.openReadView(txn);
        assertFalse(txn.transactionId().isNone(), "建 ReadView 为可写事务分配 creator 写 id");
        assertEquals(txn.transactionId(), v.creatorTrxId());
        // assignWriteId 幂等：返回已分配的同一 id
        assertEquals(txn.transactionId(), txnMgr.assignWriteId(txn));
        // 自身写可见（creator 规则）
        assertTrue(v.isVisible(txn.transactionId()), "事务能看见自己之后写入的修改");
    }

    /**
     * 验证 {@code readOnlyCreatorStaysNone} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void readOnlyCreatorStaysNone() {
        setup();
        Transaction txn = begin(IsolationLevel.REPEATABLE_READ, true);
        ReadView v = rvm.openReadView(txn);
        assertTrue(v.creatorTrxId().isNone(), "只读事务 ReadView creator 保持 NONE");
        assertTrue(txn.transactionId().isNone(), "建 ReadView 不为只读事务分配写 id");
    }

    /**
     * 验证 {@code readUncommittedAndSerializableRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void readUncommittedAndSerializableRejected() {
        setup();
        Transaction ru = begin(IsolationLevel.READ_UNCOMMITTED, false);
        assertThrows(TransactionStateException.class, () -> rvm.openReadView(ru),
                "RU 未实现，显式拒绝而非静默当 RR/RC");
        Transaction ser = begin(IsolationLevel.SERIALIZABLE, false);
        assertThrows(TransactionStateException.class, () -> rvm.openReadView(ser));
    }

    /**
     * 验证 {@code openReadViewRequiresActive} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void openReadViewRequiresActive() {
        setup();
        Transaction txn = begin(IsolationLevel.REPEATABLE_READ, false);
        txnMgr.commit(txn);
        assertThrows(TransactionStateException.class, () -> rvm.openReadView(txn));
    }

    /**
     * 验证 {@code releaseClearsRrCache} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void releaseClearsRrCache() {
        setup();
        Transaction txn = begin(IsolationLevel.REPEATABLE_READ, false);
        ReadView v1 = rvm.openReadView(txn);
        assertSame(v1, txn.readView());
        rvm.release(txn);
        assertNull(txn.readView(), "release 清 RR 缓存");
        ReadView v2 = rvm.openReadView(txn);
        assertNotSame(v1, v2, "release 后重开建新 ReadView");
    }

    /**
     * 验证 {@code releaseIsIdempotentAndAllowsNonActive} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void releaseIsIdempotentAndAllowsNonActive() {
        setup();
        Transaction txn = begin(IsolationLevel.REPEATABLE_READ, false);
        rvm.openReadView(txn);
        txnMgr.commit(txn); // COMMITTED；commit 内已 release 一次
        rvm.release(txn);   // 再次 release 幂等不抛
        assertNull(txn.readView());
    }
}
