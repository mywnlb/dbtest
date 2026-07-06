package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoCapacityThrottle;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
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

    @Test
    void beginReservesForegroundRedoBytesBeforePageLatch() {
        AtomicInteger flushRequests = new AtomicInteger();
        AtomicReference<Lsn> checkpoint = new AtomicReference<>(Lsn.of(0));
        RedoCapacityThrottle throttle = new RedoCapacityThrottle(
                RedoCapacityPolicy.fixed(100), () -> Lsn.of(70), checkpoint::get,
                flushRequests::incrementAndGet,
                () -> {
                    flushRequests.incrementAndGet();
                    checkpoint.set(Lsn.of(10));
                },
                Duration.ofMillis(1));
        MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(),
                new RedoLogManager(), throttle, 10);

        MiniTransaction mtr = mgr.begin();
        assertEquals(1, flushRequests.get(),
                "begin must check current LSN plus reserved foreground bytes before any page latch is acquired");

        mgr.commit(mtr);
    }
}
