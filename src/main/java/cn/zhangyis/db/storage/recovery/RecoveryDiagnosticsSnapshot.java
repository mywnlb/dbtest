package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;
import java.util.Optional;

/**
 * StorageEngine 暴露给上层/测试的 recovery 诊断快照。快照只包含不可变值，不暴露 gate、journal、
 * worker 或锁对象本身，避免诊断 API 反向影响 recovery 状态机。
 *
 * @param gateState 当前 recovery gate 状态。
 * @param lastReport 最近一次 recovery report；fresh open 或尚未进入 recovery 时为空。
 * @param lastFailureMessage 最近一次 gate fail closed 根因摘要；没有失败时为空。
 * @param progressEvents 当前进程内 recovery progress 事件快照。
 */
public record RecoveryDiagnosticsSnapshot(RecoveryState gateState,
                                          Optional<RecoveryReport> lastReport,
                                          Optional<String> lastFailureMessage,
                                          List<RecoveryProgressEvent> progressEvents) {

    public RecoveryDiagnosticsSnapshot {
        if (gateState == null || lastReport == null || lastFailureMessage == null || progressEvents == null) {
            throw new DatabaseValidationException("recovery diagnostics snapshot fields must not be null");
        }
        progressEvents = List.copyOf(progressEvents);
    }
}
