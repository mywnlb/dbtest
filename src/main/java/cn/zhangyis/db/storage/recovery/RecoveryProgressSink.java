package cn.zhangyis.db.storage.recovery;

/**
 * recovery progress 事件输出端口。实现只能观察事件并写入诊断介质，不能反向修改 recovery 状态、
 * 跳过阶段或参与恢复决策。
 */
@FunctionalInterface
public interface RecoveryProgressSink {

    /**
     * 追加一个 progress 事件。调用方保证事件已通过 {@link RecoveryProgressEvent} 的不变量校验；实现失败时应抛
     * 项目异常，让 recovery 总控决定是否 fail closed。
     *
     * @param event 已生成的不可变 progress 事件。
     */
    void append(RecoveryProgressEvent event);

    /**
     * 空输出端口，用于只需要内存快照的测试和旧构造路径。
     *
     * @return 不产生任何副作用的 sink。
     */
    static RecoveryProgressSink noop() {
        return event -> {
        };
    }
}
