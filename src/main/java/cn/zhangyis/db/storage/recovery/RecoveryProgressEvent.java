package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * recovery progress journal 的不可变事件快照。它面向启动诊断和测试断言，记录阶段生命周期事实，
 * 不携带可变锁、线程或底层仓储引用。
 *
 * @param sequence 单进程内单调递增序号，用于保留 stage 事件顺序；不承诺跨进程持久化。
 * @param mode 本次 recovery 请求模式。
 * @param stageName 产生事件的恢复阶段。
 * @param kind 阶段事件类型。
 * @param state 事件产生时 recovery service/gate 的语义状态。
 * @param recoveredToLsn 事件已知的 redo 恢复边界；未知时为 0，避免 null 传播到诊断层。
 * @param detail 诊断细节；成功事件可为空字符串，失败事件包含异常摘要。
 */
public record RecoveryProgressEvent(long sequence,
                                    RecoveryMode mode,
                                    RecoveryStageName stageName,
                                    RecoveryProgressEventKind kind,
                                    RecoveryState state,
                                    Lsn recoveredToLsn,
                                    String detail) {

    public RecoveryProgressEvent {
        if (sequence <= 0) {
            throw new DatabaseValidationException("recovery progress sequence must be positive: " + sequence);
        }
        if (mode == null || stageName == null || kind == null || state == null
                || recoveredToLsn == null || detail == null) {
            throw new DatabaseValidationException("recovery progress event fields must not be null");
        }
    }
}
