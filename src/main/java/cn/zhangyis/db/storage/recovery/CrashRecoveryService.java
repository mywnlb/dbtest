package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fil.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fsp.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.fsp.SpaceHeaderRawCodec;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoLogBatch;
import cn.zhangyis.db.storage.redo.RedoRecoveryReader;

import java.nio.ByteBuffer;
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

                // 先安装恢复边界，使后续 undo 续作（会 append marker/rebuild redo）从 recoveredToLsn 连续追加。
                // 与 undo 参与者内部对同一 recoveredToLsn 的安装幂等共存（RedoLogManager 同值再装为 no-op）。
                if (request.recoveredRedoManager() != null) {
                    request.recoveredRedoManager().restoreRecoveredBoundary(reader.recoveredToLsn());
                    stages.add(RecoveryStageName.REDO_BOUNDARY_INSTALL);
                }

                if (request.undoTablespaceRecovery() != null) {
                    request.undoTablespaceRecovery().resumeAfterRedo(reader.recoveredToLsn());
                    stages.add(RecoveryStageName.UNDO_TABLESPACE_RESUME);
                }

                // reconcile 必须晚于 undo 续作：续作把被截断 undo 表空间的 page0 重建为新小尺寸后，reconcile 才读到正确大小。
                if (!request.spacesToReconcile().isEmpty()) {
                    reconcileSpaceFiles(request);
                    stages.add(RecoveryStageName.SPACE_FILE_RECONCILE);
                }

                // durability 屏障：开放流量前 force 全部恢复写。replay/repair/reconcile 都绕过 Buffer Pool dirty 跟踪、
                // 自身不 fsync；若不在此落盘，一旦后续 checkpoint 越过 recoveredToLsn 并回收 redo，再次崩溃将既无 redo
                // 也无 durable 页，丢失恢复结果。force 后任何越过 recoveredToLsn 的 checkpoint 都是安全的。
                request.applyContext().pageStore().forceAll();

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

    /**
     * SPACE_FILE_RECONCILE：redo replay 后，按显式配置的表空间集，把物理文件大小重对齐到 redo 恢复出的
     * page0.currentSizeInPages。它弥补 autoExtend 已扩展并写过 page0 大小、但 extent 内仅个别页有 redo 而留下的
     * 尾部零页——这些页无 redo 描述，只能据 page0 权威逻辑大小补齐。page0（pageNo 0）恒在界内，读出后用
     * {@link SpaceHeaderRawCodec} 解出 currentSize，再调用幂等的 {@link PageStore#ensureCapacity}。
     */
    private void reconcileSpaceFiles(RecoveryRequest request) {
        PageStore pageStore = request.applyContext().pageStore();
        PageSize pageSize = request.applyContext().pageSize();
        for (SpaceId spaceId : request.spacesToReconcile()) {
            byte[] page0 = new byte[pageSize.bytes()];
            pageStore.readPage(PageId.of(spaceId, PageNo.of(0)), ByteBuffer.wrap(page0));
            SpaceHeaderPhysical header = SpaceHeaderRawCodec.readPhysical(ByteBuffer.wrap(page0));
            validateReconcileHeader(spaceId, pageSize, header);
            pageStore.ensureCapacity(spaceId, header.currentSizeInPages());
        }
    }

    /**
     * 在用 page0 的 currentSizeInPages 驱动物理扩展前做损坏校验：损坏 header 可能带任意大小，盲目据其扩展会触发
     * 错误的文件增长甚至磁盘耗尽。校验 page0 自描述的 spaceId/pageSize 与请求一致、size 为正、且字节偏移不溢出。
     * 任一不符抛 {@link TablespaceCorruptedException}，由 recover 统一 fail closed。
     *
     * <p>简化点：尚无实例级配置，未对“合理最大页数”设绝对上界；当前以 spaceId/pageSize 身份一致 + 正数 + 溢出
     * 保护拦截绝大多数损坏，绝对页数上界留待引入表空间大小配置后补充。
     */
    private void validateReconcileHeader(SpaceId spaceId, PageSize pageSize, SpaceHeaderPhysical header) {
        if (!header.spaceId().equals(spaceId)) {
            throw new TablespaceCorruptedException("reconcile page0 space id mismatch: expected="
                    + spaceId.value() + " actual=" + header.spaceId().value());
        }
        if (!header.pageSize().equals(pageSize)) {
            throw new TablespaceCorruptedException("reconcile page0 page size mismatch: expected="
                    + pageSize.bytes() + " actual=" + header.pageSize().bytes());
        }
        long pages = header.currentSizeInPages().value();
        if (pages < 1) {
            throw new TablespaceCorruptedException(
                    "reconcile page0 currentSizeInPages must be positive: " + pages);
        }
        try {
            Math.multiplyExact(pages, (long) pageSize.bytes());
        } catch (ArithmeticException overflow) {
            throw new TablespaceCorruptedException(
                    "reconcile page0 currentSizeInPages overflows byte offset: " + pages, overflow);
        }
    }

    private int repairDoublewritePages(RecoveryRequest request) {
        int repaired = 0;
        if (request.undoTablespaceRecovery() != null) {
            repaired += request.undoTablespaceRecovery().prepareDoublewrite(request.doublewriteScanner());
        }
        if (request.doublewriteScanner() == null) {
            return repaired;
        }
        for (PageId pageId : request.pagesToRepair()) {
            if (request.undoTablespaceRecovery() != null
                    && !request.undoTablespaceRecovery().shouldRepairDoublewritePage(pageId)) {
                continue;
            }
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
