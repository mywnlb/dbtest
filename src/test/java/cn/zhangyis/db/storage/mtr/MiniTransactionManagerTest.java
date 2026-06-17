package cn.zhangyis.db.storage.mtr;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MiniTransactionManager 测试固定线程绑定、禁静默嵌套、commit/rollback 解绑、跨线程拒绝。
 */
class MiniTransactionManagerTest {

    @Test
    void beginShouldActivateAndBind() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        assertEquals(MiniTransactionState.ACTIVE, mtr.state());
        assertSame(mtr, mgr.current());
        mgr.commit(mtr);
    }

    @Test
    void nestedBeginShouldThrow() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        assertThrows(MtrStateException.class, mgr::begin);
        mgr.commit(mtr);
    }

    @Test
    void currentWithoutActiveShouldThrow() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        assertThrows(MtrStateException.class, mgr::current);
    }

    @Test
    void commitShouldUnbindAndAllowRebegin() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction first = mgr.begin();
        mgr.commit(first);
        assertThrows(MtrStateException.class, mgr::current);
        MiniTransaction second = mgr.begin();
        assertEquals(MiniTransactionState.ACTIVE, second.state());
        mgr.commit(second);
    }

    @Test
    void commitUnboundTransactionShouldThrow() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        mgr.commit(mtr);
        assertThrows(MtrStateException.class, () -> mgr.commit(mtr));
    }

    @Test
    void rollbackShouldUnbind() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        mgr.rollbackUncommitted(mtr);
        assertEquals(MiniTransactionState.ROLLED_BACK, mtr.state());
        assertThrows(MtrStateException.class, mgr::current);
    }

    @Test
    void commitFromAnotherThreadShouldBeRejected() throws InterruptedException {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try {
                mgr.commit(mtr);
            } catch (Throwable t) {
                error.set(t);
            }
        });
        other.start();
        other.join();
        assertInstanceOf(MtrStateException.class, error.get());
        mgr.commit(mtr);
    }
}
