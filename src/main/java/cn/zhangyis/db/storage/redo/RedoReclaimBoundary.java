package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;

/**
 * Redo 回收边界端口：把「已持久化的 checkpoint LSN」推送给 redo 文件层，低于该 LSN 的 redo 不再为崩溃恢复所需、对应文件可复用。
 *
 * <p>由 checkpoint 侧（{@code flush.checkpoint.CheckpointCoordinator}）注入 redo 文件层（{@link RotatingRedoLogRepository}），
 * 方向为 flush→redo，符合分层依赖；redo 文件层据此回收，不反向读取 Buffer Pool / flush 状态。
 *
 * <p><b>WAL/恢复不变量</b>：调用方必须在 checkpoint label 已 durable 之后才推进该边界。若先放开回收再持久 checkpoint，
 * 崩溃后恢复可能从更旧的 checkpoint 重放，却发现所需 redo 已被新一代覆盖。
 */
@FunctionalInterface
public interface RedoReclaimBoundary {

    /**
     * 推进回收边界到已持久化的 checkpoint LSN（实现需保证单调，不回退）。
     *
     * @param checkpointLsn 已持久化的 checkpoint LSN。
     */
    void advanceReclaimBoundary(Lsn checkpointLsn);
}
