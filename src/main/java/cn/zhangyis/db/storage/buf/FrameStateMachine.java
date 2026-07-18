package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

/**
 * 帧状态机（设计 §5.7）：集中校验并执行 {@link BufferFrame#state} 的合法转换，取代多处直接改字段。
 * 所有调用都在目标 frame 的 frameMutex 下串行，故状态机本身无需加锁；它只做"校验 + 赋值"，不持有帧的任何其它字段。
 *
 * <p><b>合法转换表</b>（self-transition 恒合法，便于幂等 markDirty/clean 复调）：
 * <ul>
 *   <li>FREE → LOADING（认领待读）/ CLEAN（直接装入，如 newPage 清零或干净帧复用）。</li>
 *   <li>LOADING → CLEAN（读盘成功发布）/ FREE（读盘失败回收占位）。</li>
 *   <li>CLEAN → DIRTY_PENDING（MTR 写入）/ DIRTY（直接发布）/ EVICTING（淘汰）/ STALE（生命周期失效）/ LOADING（复用）。</li>
 *   <li>DIRTY_PENDING → DIRTY（MTR 发布）/ CLEAN（未发布写入回滚）。</li>
 *   <li>DIRTY → FLUSHING（开始刷盘）/ CLEAN（legacy 直写回清脏）。</li>
 *   <li>FLUSHING → CLEAN（刷盘成功）/ DIRTY（刷盘失败或刷盘期又被改）。</li>
 *   <li>EVICTING → FREE/LOADING，STALE → FREE。</li>
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
                EnumSet.of(BufferFrameState.DIRTY_PENDING, BufferFrameState.DIRTY,
                        BufferFrameState.FREE, BufferFrameState.LOADING, BufferFrameState.EVICTING,
                        BufferFrameState.STALE));
        ALLOWED.put(BufferFrameState.DIRTY_PENDING,
                EnumSet.of(BufferFrameState.DIRTY, BufferFrameState.CLEAN));
        ALLOWED.put(BufferFrameState.DIRTY, EnumSet.of(BufferFrameState.FLUSHING, BufferFrameState.CLEAN));
        ALLOWED.put(BufferFrameState.FLUSHING, EnumSet.of(BufferFrameState.CLEAN, BufferFrameState.DIRTY));
        ALLOWED.put(BufferFrameState.EVICTING, EnumSet.of(BufferFrameState.FREE, BufferFrameState.LOADING));
        ALLOWED.put(BufferFrameState.STALE, EnumSet.of(BufferFrameState.FREE));
    }

    /**
     * 校验并执行一次状态转换（调用须持目标 frameMutex）。to 与当前相同（幂等）或在合法后继表中则赋值，否则抛
     * {@link DatabaseValidationException} 且保持原状态不变。
     * <p>数据流：</p>
     * <ol>
     *     <li>按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，过期映射在返回前拒绝。</li>
     *     <li>遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，慢 IO 或条件等待移到内部锁外。</li>
     *     <li>完成页载入、替换、dirty snapshot 或状态转换，并向等待者发布唯一完成或失败信号。</li>
     *     <li>返回受控 Guard/快照或释放 fix；失败回收占位且不错误清除并发产生的 dirty 状态。</li>
     * </ol>
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param to 选择 {@code transition} 分支的 {@code BufferFrameState} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void transition(BufferFrame frame, BufferFrameState to) {
        // 1、按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，在共享或持久副作用前拒绝非法状态。
        if (frame == null || to == null) {
            throw new DatabaseValidationException("frame and target state must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，保持处理顺序与资源边界。
        BufferFrameState from = frame.state;
        if (from == to) {
            return;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，完成页载入、替换、dirty snapshot 或状态转换，并维持领域不变量。
        EnumSet<BufferFrameState> successors = ALLOWED.get(from);
        if (successors == null || !successors.contains(to)) {
            throw new DatabaseValidationException(
                    "illegal buffer frame state transition: " + from + " -> " + to);
        }
        // 4、返回受控 Guard/快照或释放 fix，以稳定返回或领域异常完成收口。
        frame.state = to;
    }
}
