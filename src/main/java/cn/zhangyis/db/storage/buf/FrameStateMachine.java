package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

/**
 * 帧状态机（设计 §5.7）：集中校验并执行 {@link BufferFrame#state} 的合法转换，取代多处直接改字段。
 * 所有调用都在 BufferPool 的 poolLock 下串行，故状态机本身无需加锁；它只做"校验 + 赋值"，不持有帧的任何其它字段。
 *
 * <p><b>合法转换表</b>（self-transition 恒合法，便于幂等 markDirty/clean 复调）：
 * <ul>
 *   <li>FREE → LOADING（认领待读）/ CLEAN（直接装入，如 newPage 清零或干净帧复用）。</li>
 *   <li>LOADING → CLEAN（读盘成功发布）/ FREE（读盘失败回收占位）。</li>
 *   <li>CLEAN → DIRTY（标脏）/ FREE（淘汰干净帧）/ LOADING（复用干净 victim 载入新页）。</li>
 *   <li>DIRTY → FLUSHING（开始刷盘）/ CLEAN（legacy 直写回清脏）。</li>
 *   <li>FLUSHING → CLEAN（刷盘成功）/ DIRTY（刷盘失败或刷盘期又被改）。</li>
 * </ul>
 * 不在表中的转换（如 FREE→FLUSHING、LOADING→DIRTY、FLUSHING→FREE）视为不变量被破坏，抛
 * {@link DatabaseValidationException}，且不改变帧状态——便于在测试与生产中尽早暴露错误的生命周期编排。
 */
final class FrameStateMachine {

    /** 合法后继表；不含 self-transition（由 {@link #transition} 单独放行）。 */
    private static final Map<BufferFrameState, EnumSet<BufferFrameState>> ALLOWED =
            new EnumMap<>(BufferFrameState.class);

    static {
        ALLOWED.put(BufferFrameState.FREE, EnumSet.of(BufferFrameState.LOADING, BufferFrameState.CLEAN));
        ALLOWED.put(BufferFrameState.LOADING, EnumSet.of(BufferFrameState.CLEAN, BufferFrameState.FREE));
        ALLOWED.put(BufferFrameState.CLEAN,
                EnumSet.of(BufferFrameState.DIRTY, BufferFrameState.FREE, BufferFrameState.LOADING));
        ALLOWED.put(BufferFrameState.DIRTY, EnumSet.of(BufferFrameState.FLUSHING, BufferFrameState.CLEAN));
        ALLOWED.put(BufferFrameState.FLUSHING, EnumSet.of(BufferFrameState.CLEAN, BufferFrameState.DIRTY));
    }

    /**
     * 校验并执行一次状态转换（调用须持 poolLock）。to 与当前相同（幂等）或在合法后继表中则赋值，否则抛
     * {@link DatabaseValidationException} 且保持原状态不变。
     */
    void transition(BufferFrame frame, BufferFrameState to) {
        if (frame == null || to == null) {
            throw new DatabaseValidationException("frame and target state must not be null");
        }
        BufferFrameState from = frame.state;
        if (from == to) {
            return;
        }
        EnumSet<BufferFrameState> successors = ALLOWED.get(from);
        if (successors == null || !successors.contains(to)) {
            throw new DatabaseValidationException(
                    "illegal buffer frame state transition: " + from + " -> " + to);
        }
        frame.state = to;
    }
}
