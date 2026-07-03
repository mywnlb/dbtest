package cn.zhangyis.db.server.lockobs.snapshot;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.server.lockobs.domain.WaitSlotSnapshot;
import cn.zhangyis.db.server.lockobs.report.DeadlockReport;

import java.util.List;

/**
 * 一次 lockobs 当前快照。快照带 epoch，所有列表均为不可变副本；它不承诺全局暂停式一致性，
 * 只保证每行来自 LockManager/registry 的受保护读取。
 *
 * @param epoch          快照版本。
 * @param dataLocks      当前 data_locks 行。
 * @param dataLockWaits  当前 data_lock_waits 行。
 * @param waitSlots      当前线程等待槽。
 * @param deadlockReports 最近 deadlock report，最新在前。
 * @param partial        是否为部分采集。
 * @param truncated      是否因 maxRows 被裁剪。
 */
public record LockDiagnosticSnapshot(long epoch, List<DataLockRow> dataLocks,
                                     List<DataLockWaitRow> dataLockWaits,
                                     List<WaitSlotSnapshot> waitSlots,
                                     List<DeadlockReport> deadlockReports,
                                     boolean partial, boolean truncated) {

    public LockDiagnosticSnapshot {
        if (epoch < 0) {
            throw new DatabaseValidationException("snapshot epoch must be non-negative");
        }
        if (dataLocks == null || dataLockWaits == null || waitSlots == null || deadlockReports == null) {
            throw new DatabaseValidationException("snapshot lists must not be null");
        }
        dataLocks = List.copyOf(dataLocks);
        dataLockWaits = List.copyOf(dataLockWaits);
        waitSlots = List.copyOf(waitSlots);
        deadlockReports = List.copyOf(deadlockReports);
    }
}
