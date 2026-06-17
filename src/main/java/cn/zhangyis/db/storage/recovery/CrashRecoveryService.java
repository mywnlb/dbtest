package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoLogBatch;
import cn.zhangyis.db.storage.redo.RedoRecoveryReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * R2 crash recovery 启动门面。它只负责编排阶段顺序：关闭流量、doublewrite repair、checkpoint-aware redo replay、
 * 成功后开放流量；事务 rollback、DDL recovery、purge resume 是后续阶段，不在本类中伪实现。
 */
public final class CrashRecoveryService {

    /** 防止同一个 service 并发执行两次启动恢复。 */
    private final ReentrantLock serviceLock = new ReentrantLock();
    /** 用户流量门控。 */
    private final RecoveryTrafficGate gate;
    /** 最近一次恢复状态。 */
    private RecoveryState state = RecoveryState.CLOSED;
    /** 最近一次成功恢复报告。 */
    private RecoveryReport lastReport;
    /** 最近一次失败根因。 */
    private Throwable lastError;

    public CrashRecoveryService(RecoveryTrafficGate gate) {
        if (gate == null) {
            throw new DatabaseValidationException("recovery traffic gate must not be null");
        }
        this.gate = gate;
    }

    /**
     * 执行 R2 最小恢复链。任何阶段失败都会 fail closed 并抛出 {@link RecoveryStartupException}，避免带未完成恢复的存储层开放流量。
     *
     * @param request 恢复请求。
     * @return 恢复报告。
     */
    public RecoveryReport recover(RecoveryRequest request) {
        if (request == null) {
            throw new DatabaseValidationException("recovery request must not be null");
        }
        serviceLock.lock();
        try {
            List<RecoveryStageName> stages = new ArrayList<>();
            try {
                state = RecoveryState.RECOVERING;
                gate.closeForRecovery();
                stages.add(RecoveryStageName.TRAFFIC_CLOSED);

                int repaired = repairDoublewritePages(request);
                stages.add(RecoveryStageName.DOUBLEWRITE_REPAIR);

                RedoCheckpointLabel checkpoint = request.checkpointStore().readLatest();
                RedoRecoveryReader reader = new RedoRecoveryReader(request.redoRepository(), checkpoint.checkpointLsn());
                List<RedoLogBatch> batches = reader.readBatches();
                request.dispatcher().applyAll(batches, request.applyContext());
                stages.add(RecoveryStageName.REDO_REPLAY);

                gate.openForUserTraffic();
                stages.add(RecoveryStageName.OPEN_TRAFFIC);
                state = RecoveryState.OPEN;
                RecoveryReport report = new RecoveryReport(request.mode(), state, checkpoint.checkpointLsn(),
                        reader.recoveredToLsn(), repaired, batches.size(), stages);
                lastReport = report;
                lastError = null;
                return report;
            } catch (DatabaseRuntimeException e) {
                failClosed(request.mode(), e);
                throw new RecoveryStartupException("crash recovery failed before user traffic opened", e);
            } catch (RuntimeException e) {
                failClosed(request.mode(), e);
                throw new RecoveryStartupException("crash recovery failed with unexpected runtime error", e);
            }
        } finally {
            serviceLock.unlock();
        }
    }

    /** 当前 service 状态。 */
    public RecoveryState state() {
        serviceLock.lock();
        try {
            return state;
        } finally {
            serviceLock.unlock();
        }
    }

    /** 最近一次成功恢复报告。 */
    public Optional<RecoveryReport> lastReport() {
        serviceLock.lock();
        try {
            return Optional.ofNullable(lastReport);
        } finally {
            serviceLock.unlock();
        }
    }

    /** 最近一次失败根因。 */
    public Optional<Throwable> lastError() {
        serviceLock.lock();
        try {
            return Optional.ofNullable(lastError);
        } finally {
            serviceLock.unlock();
        }
    }

    private int repairDoublewritePages(RecoveryRequest request) {
        if (request.doublewriteScanner() == null) {
            return 0;
        }
        int repaired = 0;
        for (PageId pageId : request.pagesToRepair()) {
            if (request.doublewriteScanner().repairPageIfNeeded(pageId)) {
                repaired++;
            }
        }
        return repaired;
    }

    /**
     * 任意阶段失败时保持 gate 关闭并记录失败快照。透传发起恢复时的 {@code mode}，避免失败诊断报告把非 NORMAL 模式
     * 误记为 NORMAL。报告里的 LSN/计数填 0：失败可能发生在尚未读出 checkpoint 或扫描 redo 之前，无可信进度可报。
     */
    private void failClosed(RecoveryMode mode, Throwable error) {
        gate.failClosed(error);
        state = RecoveryState.FAILED;
        lastError = error;
        lastReport = new RecoveryReport(mode, RecoveryState.FAILED,
                Lsn.of(0), Lsn.of(0), 0, 0, List.of());
    }
}
