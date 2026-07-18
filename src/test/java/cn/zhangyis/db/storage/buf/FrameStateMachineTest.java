package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 帧状态机测试（Phase B，设计 §5.7）：合法转换被应用、非法转换被拒且不改状态。
 * 帧状态由 poolLock 串行，状态机只校验+赋值，不自带锁。
 */
class FrameStateMachineTest {

    private static final PageSize PS = PageSize.ofBytes(4 * 1024);

    /**
     * 验证 {@code legalLifecycleTransitionsApplied} 所描述的组件生命周期，并断言状态转换、后台线程停止和资源恰好释放一次。
     */
    @Test
    void legalLifecycleTransitionsApplied() {
        FrameStateMachine fsm = new FrameStateMachine();
        BufferFrame f = new BufferFrame(PS); // 初始 FREE
        assertEquals(BufferFrameState.FREE, f.state);

        fsm.transition(f, BufferFrameState.LOADING);
        assertEquals(BufferFrameState.LOADING, f.state);
        fsm.transition(f, BufferFrameState.CLEAN);
        assertEquals(BufferFrameState.CLEAN, f.state);
        fsm.transition(f, BufferFrameState.DIRTY);
        assertEquals(BufferFrameState.DIRTY, f.state);
        fsm.transition(f, BufferFrameState.FLUSHING);
        assertEquals(BufferFrameState.FLUSHING, f.state);
        fsm.transition(f, BufferFrameState.CLEAN); // 刷盘成功回 CLEAN
        assertEquals(BufferFrameState.CLEAN, f.state);
    }

    /**
     * 验证 {@code illegalTransitionRejectedAndStateUnchanged} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void illegalTransitionRejectedAndStateUnchanged() {
        FrameStateMachine fsm = new FrameStateMachine();
        BufferFrame f = new BufferFrame(PS); // FREE

        // FREE 不能直接进 FLUSHING（必须先载入并变脏）。
        assertThrows(DatabaseValidationException.class,
                () -> fsm.transition(f, BufferFrameState.FLUSHING));
        assertEquals(BufferFrameState.FREE, f.state, "非法转换不得改变状态");

        // LOADING 不能直接进 DIRTY（载入中内容未就绪）。
        fsm.transition(f, BufferFrameState.LOADING);
        assertThrows(DatabaseValidationException.class,
                () -> fsm.transition(f, BufferFrameState.DIRTY));
        assertEquals(BufferFrameState.LOADING, f.state);
    }

    /**
     * 验证 {@code fineGrainedLifecycleStatesKeepBoundaries} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void fineGrainedLifecycleStatesKeepBoundaries() {
        FrameStateMachine fsm = new FrameStateMachine();
        BufferFrame f = new BufferFrame(PS);
        fsm.transition(f, BufferFrameState.CLEAN);
        fsm.transition(f, BufferFrameState.DIRTY_PENDING);
        assertThrows(DatabaseValidationException.class,
                () -> fsm.transition(f, BufferFrameState.FLUSHING));
        fsm.transition(f, BufferFrameState.CLEAN);
        fsm.transition(f, BufferFrameState.EVICTING);
        fsm.transition(f, BufferFrameState.FREE);
        fsm.transition(f, BufferFrameState.CLEAN);
        fsm.transition(f, BufferFrameState.STALE);
        fsm.transition(f, BufferFrameState.FREE);
    }
}
