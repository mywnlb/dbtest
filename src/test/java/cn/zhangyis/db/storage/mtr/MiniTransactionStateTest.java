package cn.zhangyis.db.storage.mtr;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MiniTransactionState 测试固定 MTR 生命周期状态机，避免 begin/commit/rollback 各自乱改状态。
 */
class MiniTransactionStateTest {

    /**
     * 验证 {@code shouldAllowLifecycleTransitions} 所描述的组件生命周期，并断言状态转换、后台线程停止和资源恰好释放一次。
     */
    @Test
    void shouldAllowLifecycleTransitions() {
        assertTrue(MiniTransactionState.NEW.canTransitTo(MiniTransactionState.ACTIVE));
        assertTrue(MiniTransactionState.ACTIVE.canTransitTo(MiniTransactionState.COMMITTING));
        assertTrue(MiniTransactionState.ACTIVE.canTransitTo(MiniTransactionState.ROLLED_BACK));
        assertTrue(MiniTransactionState.COMMITTING.canTransitTo(MiniTransactionState.COMMITTED));
    }

    /**
     * 验证 {@code shouldRejectIllegalTransitions} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
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
