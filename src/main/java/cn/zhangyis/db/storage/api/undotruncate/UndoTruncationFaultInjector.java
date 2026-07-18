package cn.zhangyis.db.storage.api.undotruncate;

/** undo truncate 故障注入钩子；只在显式持久化边界后调用，不改变生产流程控制。 */
@FunctionalInterface
public interface UndoTruncationFaultInjector {

    /**
     * 接收 {@code after} 对应的存储引擎稳定 API生命周期事件；只更新本策略拥有的统计或顺序状态，不接管事件来源资源。
     *
     * @param phase 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     */
    void after(UndoTruncationPhase phase);

    static UndoTruncationFaultInjector none() {
        return ignored -> { };
    }
}
