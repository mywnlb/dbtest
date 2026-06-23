package cn.zhangyis.db.storage.api;

/** undo truncate 故障注入钩子；只在显式持久化边界后调用，不改变生产流程控制。 */
@FunctionalInterface
public interface UndoTruncationFaultInjector {

    void after(UndoTruncationPhase phase);

    static UndoTruncationFaultInjector none() {
        return ignored -> { };
    }
}
