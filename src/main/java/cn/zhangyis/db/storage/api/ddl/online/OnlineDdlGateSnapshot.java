package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.util.Optional;

/**
 * 指定 table gate 在一次短锁内复制的弱一致诊断投影。它不泄漏 transaction map、capture target
 * 或 change-log 引用，诊断端不能通过快照改变 admission。
 *
 * @param tableId 被观察的正 table identity
 * @param buildId 当前 operation identity；ABSENT 时为空
 * @param phase 当前 gate 运行态
 * @param inFlightAdmissions 已进入但尚未关闭的短 DML admission 数
 * @param ioLeases 正在 gate 锁外追加或 force row-log 的 I/O lease 数
 * @param activeTransactions 尚未发布 commit/rollback 终态且引用该表的事务数
 * @param terminalRedoHighWater 已终态 committed writer 的最大 redo LSN
 */
public record OnlineDdlGateSnapshot(
        long tableId,
        Optional<OnlineIndexBuildId> buildId,
        OnlineDdlTablePhase phase,
        long inFlightAdmissions,
        long ioLeases,
        long activeTransactions,
        Lsn terminalRedoHighWater) {

    public OnlineDdlGateSnapshot {
        if (tableId <= 0 || buildId == null || phase == null
                || (phase == OnlineDdlTablePhase.ABSENT) != buildId.isEmpty()
                || inFlightAdmissions < 0 || ioLeases < 0 || activeTransactions < 0
                || terminalRedoHighWater == null) {
            throw new DatabaseValidationException("invalid Online DDL gate snapshot");
        }
    }
}
