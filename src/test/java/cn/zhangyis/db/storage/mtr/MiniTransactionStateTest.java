package cn.zhangyis.db.storage.mtr;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MiniTransactionState 测试固定 MTR 生命周期状态机，避免 begin/commit/rollback 各自乱改状态。
 */
class MiniTransactionStateTest {

    @Test
    void shouldAllowLifecycleTransitions() {
        assertTrue(MiniTransactionState.NEW.canTransitTo(MiniTransactionState.ACTIVE));
        assertTrue(MiniTransactionState.ACTIVE.canTransitTo(MiniTransactionState.COMMITTING));
        assertTrue(MiniTransactionState.ACTIVE.canTransitTo(MiniTransactionState.ROLLED_BACK));
        assertTrue(MiniTransactionState.COMMITTING.canTransitTo(MiniTransactionState.COMMITTED));
    }

    @Test
    void shouldRejectIllegalTransitions() {
        assertFalse(MiniTransactionState.NEW.canTransitTo(MiniTransactionState.COMMITTING));
        assertFalse(MiniTransactionState.ACTIVE.canTransitTo(MiniTransactionState.COMMITTED));
        assertFalse(MiniTransactionState.COMMITTED.canTransitTo(MiniTransactionState.ACTIVE));
        assertFalse(MiniTransactionState.ROLLED_BACK.canTransitTo(MiniTransactionState.ACTIVE));
        assertThrows(MtrStateException.class,
                () -> MiniTransactionState.COMMITTED.validateTransitTo(MiniTransactionState.ACTIVE));
    }
}
