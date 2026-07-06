package cn.zhangyis.db.storage.recovery;

/**
 * recovery progress journal 中单条事件的类型。事件只描述恢复阶段生命周期，不代表用户流量是否可进入；
 * 真实门控仍以 {@link RecoveryTrafficGate#state()} 为准。
 */
public enum RecoveryProgressEventKind {
    /** 阶段即将执行，后续若失败可定位失败发生在哪个阶段内。 */
    STARTED,
    /** 阶段已经成功完成，并写入 {@link RecoveryReport#completedStages()} 的同一阶段语义。 */
    COMPLETED,
    /** 阶段执行失败，恢复服务随后会 fail closed。 */
    FAILED
}
