package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryResult;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRawCodec;
import cn.zhangyis.db.storage.redo.RedoApplySummary;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoLogBatch;
import cn.zhangyis.db.storage.redo.RedoLogFormatException;
import cn.zhangyis.db.storage.redo.RedoRecoveryReader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * R2 crash recovery 启动门面。它只负责编排阶段顺序：关闭流量、doublewrite repair、checkpoint-aware redo replay、
 * 事务表快照/undo rollback/purge resume，成功后开放流量；具体 page3、B+Tree、history 语义由参与者实现，
 * DDL recovery 仍未接入。
 */
public final class CrashRecoveryService {

    /** 防止同一个 service 并发执行两次启动恢复。 */
    private final ReentrantLock serviceLock = new ReentrantLock();
    /** 用户流量门控。 */
    private final RecoveryTrafficGate gate;
    /** recovery 进度 journal；只记录阶段生命周期事实，不参与恢复决策。 */
    private final RecoveryProgressJournal progressJournal;
    /** 最近一次恢复状态。 */
    private RecoveryState state = RecoveryState.CLOSED;
    /** 最近一次成功恢复报告。 */
    private RecoveryReport lastReport;
    /** 最近一次失败根因。 */
    private Throwable lastError;

    public CrashRecoveryService(RecoveryTrafficGate gate) {
        this(gate, new RecoveryProgressJournal());
    }

    public CrashRecoveryService(RecoveryTrafficGate gate, RecoveryProgressJournal progressJournal) {
        if (gate == null) {
            throw new DatabaseValidationException("recovery traffic gate must not be null");
        }
        if (progressJournal == null) {
            throw new DatabaseValidationException("recovery progress journal must not be null");
        }
        this.gate = gate;
        this.progressJournal = progressJournal;
    }

    /**
     * 执行 R2 最小恢复链。任何阶段失败都会 fail closed 并抛出 {@link RecoveryStartupException}，避免带未完成恢复的存储层开放流量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验请求并独占恢复门面，关闭用户流量后读取 checkpoint；失败时 gate 保持关闭。</li>
     *     <li>先修复 doublewrite，再从 checkpoint 连续重放 redo，并把事务 delta 汇入不可变恢复快照。</li>
     *     <li>安装 redo 续写边界，续作 UNDO tablespace truncate，并按恢复后的 page0 对齐物理文件长度。</li>
     *     <li>调用事务参与者回滚 ACTIVE undo、恢复 committed history；该阶段不得持有 redo reader 或数据页 latch。</li>
     *     <li>在独立 RESUME_PURGE 阶段推进 persistent history，并 flush 恢复期间新产生的 rollback/purge redo。</li>
     *     <li>force 所有恢复写，保证后续 checkpoint 越过恢复边界时数据页已经 durable。</li>
     *     <li>仅在全部阶段成功后开放流量并发布报告；任一项目异常或意外运行时异常都 fail-closed。</li>
     * </ol>
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
            RecoveryStageTracker tracker = new RecoveryStageTracker(request.mode(), progressJournal, stages);
            try {
                // 1、先关闭流量并固定 checkpoint 输入，后续任何失败都不能越过 gate。
                tracker.begin(RecoveryStageName.TRAFFIC_CLOSED);
                state = RecoveryState.RECOVERING;
                gate.closeForRecovery();
                tracker.complete(state, Lsn.of(0));

                RedoCheckpointLabel checkpoint = request.checkpointStore().readLatest();
                validateRedoFormat(request, checkpoint);

                if (request.mode() == RecoveryMode.READ_ONLY_VALIDATE) {
                    return recoverReadOnlyValidate(request, tracker, checkpoint);
                }

                // 2、doublewrite 修复先于 redo；事务 delta 与 redo replay 使用同一连续边界。
                tracker.begin(RecoveryStageName.DOUBLEWRITE_REPAIR);
                DoublewriteRepairSummary doublewriteSummary = repairDoublewritePages(request);
                tracker.complete(state, Lsn.of(0), skipDetail("skippedDoublewritePages",
                        doublewriteSummary.skippedPageCount(), request.skipPolicy()));

                if (request.transactionRecoveryContext() != null) {
                    // sidecar 必须在任何 TRX_STATE_DELTA 交付前校验并建立 counter 基线；dispatcher 中的 sink
                    // 引用同一 context，故 applyAll 完成后 snapshot 包含完整 post-checkpoint 顺序证据。
                    request.transactionRecoveryContext().initialize(checkpoint.checkpointLsn());
                }
                RedoRecoveryReader reader = new RedoRecoveryReader(request.redoRepository(), checkpoint.checkpointLsn());
                tracker.begin(RecoveryStageName.REDO_REPLAY);
                List<RedoLogBatch> batches = reader.readBatches();
                if (request.transactionRecoveryContext() != null) {
                    request.transactionRecoveryContext().verifyRedoCoverage(reader.recoveredToLsn());
                }
                RedoApplySummary redoSummary = request.dispatcher().applyAll(batches, request.applyContext(),
                        request.skipPolicy()::shouldSkip);
                tracker.complete(state, reader.recoveredToLsn(), skipDetail("skippedRedoRecords",
                        redoSummary.skippedRecordCount(), request.skipPolicy()));

                // 3、先安装恢复边界，使后续 undo 续作（会 append marker/rebuild redo）从 recoveredToLsn 连续追加。
                // 与 undo 参与者内部对同一 recoveredToLsn 的安装幂等共存（RedoLogManager 同值再装为 no-op）。
                if (request.recoveredRedoManager() != null) {
                    tracker.begin(RecoveryStageName.REDO_BOUNDARY_INSTALL);
                    request.recoveredRedoManager().restoreRecoveredBoundary(reader.recoveredToLsn());
                    tracker.complete(state, reader.recoveredToLsn());
                }

                if (request.undoTablespaceRecovery() != null) {
                    tracker.begin(RecoveryStageName.UNDO_TABLESPACE_RESUME);
                    request.undoTablespaceRecovery().resumeAfterRedo(reader.recoveredToLsn());
                    tracker.complete(state, reader.recoveredToLsn());
                }

                // reconcile 必须晚于 undo 续作：续作把被截断 undo 表空间的 page0 重建为新小尺寸后，reconcile 才读到正确大小。
                int skippedReconcileSpaceCount = 0;
                if (!request.spacesToReconcile().isEmpty()) {
                    tracker.begin(RecoveryStageName.SPACE_FILE_RECONCILE);
                    skippedReconcileSpaceCount = reconcileSpaceFiles(request);
                    tracker.complete(state, reader.recoveredToLsn(), skipDetail("skippedReconcileSpaces",
                            skippedReconcileSpaceCount, request.skipPolicy()));
                }

                if (request.transactionUndoRecovery() != null) {
                    // 4、恢复 committed history、决议 PREPARED、再回滚 ACTIVE；返回时所有 undo/page latch 已释放。
                    tracker.begin(RecoveryStageName.UNDO_ROLLBACK);
                    RecoveredTransactionSnapshot transactionSnapshot =
                            request.transactionRecoveryContext().snapshot();
                    TransactionUndoRecoveryResult undoResult =
                            request.transactionUndoRecovery().recoverAfterRedo(
                                    reader.recoveredToLsn(), transactionSnapshot);
                    if (undoResult == null) {
                        throw new DatabaseValidationException("transaction undo recovery result must not be null");
                    }
                    tracker.complete(state, reader.recoveredToLsn());

                    // 5、在独立阶段真实推进恢复出的 history；异常直接阻止 OPEN_TRAFFIC。
                    tracker.begin(RecoveryStageName.RESUME_PURGE);
                    request.transactionUndoRecovery().resumePurgeAfterRedo();
                    tracker.complete(state, reader.recoveredToLsn());
                    if (request.recoveredRedoManager() != null) {
                        request.recoveredRedoManager().flush();
                    }
                }

                // 6、durability 屏障：开放流量前 force 全部恢复写。replay/repair/reconcile 都绕过 Buffer Pool dirty 跟踪、
                // 自身不 fsync；若不在此落盘，一旦后续 checkpoint 越过 recoveredToLsn 并回收 redo，再次崩溃将既无 redo
                // 也无 durable 页，丢失恢复结果。force 后任何越过 recoveredToLsn 的 checkpoint 都是安全的。
                request.applyContext().pageStore().forceAll();

                // 7、只有前六阶段全部成功才开放流量；catch 分支统一记录失败并关闭 gate。
                tracker.begin(RecoveryStageName.OPEN_TRAFFIC);
                gate.openForUserTraffic();
                state = RecoveryState.OPEN;
                tracker.complete(state, reader.recoveredToLsn());
                RecoveryReport report = new RecoveryReport(request.mode(), state, checkpoint.checkpointLsn(),
                        reader.recoveredToLsn(), doublewriteSummary.repairedPageCount(),
                        doublewriteSummary.detectedOnlyPageCount(), redoSummary.appliedBatchCount(), stages,
                        request.skipPolicy().skippedSpaces(), doublewriteSummary.skippedPageCount(),
                        redoSummary.skippedRecordCount(), skippedReconcileSpaceCount);
                lastReport = report;
                lastError = null;
                return report;
            } catch (DatabaseRuntimeException e) {
                recordFailureProgress(tracker, e);
                failClosed(request, e);
                throw new RecoveryStartupException("crash recovery failed before user traffic opened", e);
            } catch (RuntimeException e) {
                recordFailureProgress(tracker, e);
                failClosed(request, e);
                throw new RecoveryStartupException("crash recovery failed with unexpected runtime error", e);
            }
        } finally {
            serviceLock.unlock();
        }
    }

    /**
     * READ_ONLY_VALIDATE 恢复分支：只扫描 doublewrite 与 redo 边界，并发布只读诊断态。该路径故意跳过
     * {@code RedoApplyDispatcher.applyAll}、redo 边界安装、undo 续作、空间 reconcile、事务 rollback 和
     * {@code PageStore.forceAll}，确保调用方可以用同一份文件做灾难诊断而不改变磁盘内容。
     */
    private RecoveryReport recoverReadOnlyValidate(RecoveryRequest request, RecoveryStageTracker tracker,
                                                   RedoCheckpointLabel checkpoint) {
        tracker.begin(RecoveryStageName.DOUBLEWRITE_REPAIR);
        DoublewriteRepairSummary doublewriteSummary = validateDoublewritePages(request);
        tracker.complete(state, Lsn.of(0));

        if (request.transactionRecoveryContext() != null) {
            // 只读诊断仍必须证明 sidecar 覆盖 redo checkpoint；initialize 只构造私有内存表，既不 apply delta，
            // 也不发布 counter/rollback/history 或写文件。
            request.transactionRecoveryContext().initialize(checkpoint.checkpointLsn());
        }
        RedoRecoveryReader reader = new RedoRecoveryReader(request.redoRepository(), checkpoint.checkpointLsn());
        tracker.begin(RecoveryStageName.REDO_REPLAY);
        List<RedoLogBatch> batches = reader.readBatches();
        if (request.transactionRecoveryContext() != null) {
            request.transactionRecoveryContext().verifyRedoCoverage(reader.recoveredToLsn());
            request.transactionRecoveryContext().validateTransactionDeltas(batches);
        }
        tracker.complete(state, reader.recoveredToLsn());

        tracker.begin(RecoveryStageName.READ_ONLY_DIAGNOSTIC_OPEN);
        gate.enterReadOnlyDiagnostic();
        state = RecoveryState.READ_ONLY;
        tracker.complete(state, reader.recoveredToLsn());
        RecoveryReport report = new RecoveryReport(request.mode(), state, checkpoint.checkpointLsn(),
                reader.recoveredToLsn(), 0, doublewriteSummary.detectedOnlyPageCount(), 0,
                tracker.completedStages());
        lastReport = report;
        lastError = null;
        return report;
    }

    /**
     * 在 doublewrite/page apply 等任何恢复写发生前，校验 redo-control 与 redo data 的持久格式属于同一代。
     * 本切片明确拒绝旧 RLG1/control v1 和双格式猜测；若 label 与 repository 声明不一致，恢复必须 fail closed，
     * 不能把同一 LSN 按错误的物理编码解释。
     */
    private static void validateRedoFormat(RecoveryRequest request, RedoCheckpointLabel checkpoint) {
        int controlFormat = checkpoint.redoFormatVersion();
        int dataFormat = request.redoRepository().formatVersion();
        if (controlFormat != dataFormat) {
            throw new RedoLogFormatException("redo control/data format mismatch: control="
                    + controlFormat + " data=" + dataFormat);
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
    private int reconcileSpaceFiles(RecoveryRequest request) {
        PageStore pageStore = request.applyContext().pageStore();
        PageSize pageSize = request.applyContext().pageSize();
        int skipped = 0;
        for (SpaceId spaceId : request.spacesToReconcile()) {
            if (request.skipPolicy().shouldSkip(spaceId)) {
                skipped++;
                continue;
            }
            byte[] page0 = new byte[pageSize.bytes()];
            pageStore.readPage(PageId.of(spaceId, PageNo.of(0)), ByteBuffer.wrap(page0));
            SpaceHeaderPhysical header = SpaceHeaderRawCodec.readPhysical(ByteBuffer.wrap(page0));
            validateReconcileHeader(spaceId, pageSize, header);
            pageStore.ensureCapacity(spaceId, header.currentSizeInPages());
        }
        return skipped;
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

    private DoublewriteRepairSummary repairDoublewritePages(RecoveryRequest request) {
        int repaired = 0;
        int detectedOnly = 0;
        int skipped = 0;
        if (request.undoTablespaceRecovery() != null) {
            repaired += request.undoTablespaceRecovery().prepareDoublewrite(request.doublewriteScanner());
        }
        if (request.doublewriteScanner() == null) {
            return new DoublewriteRepairSummary(repaired, detectedOnly, skipped);
        }
        for (PageId pageId : request.pagesToRepair()) {
            if (request.skipPolicy().shouldSkip(pageId)) {
                skipped++;
                continue;
            }
            if (request.undoTablespaceRecovery() != null
                    && !request.undoTablespaceRecovery().shouldRepairDoublewritePage(pageId)) {
                continue;
            }
            DoublewriteRecoveryResult result = request.doublewriteScanner().scanPageIfNeeded(pageId);
            if (result.repaired()) {
                repaired++;
            } else if (result.detectedOnly()) {
                detectedOnly++;
            }
        }
        return new DoublewriteRepairSummary(repaired, detectedOnly, skipped);
    }

    /**
     * READ_ONLY_VALIDATE 的 doublewrite 阶段：只调用 scanner 的只读校验入口，统计 full-copy 可修复页和 detect-only
     * 可疑页，但绝不调用 undo participant、PageStore.writePage 或 force。这样诊断报告能暴露 torn-page 风险，同时
     * 保持原 data file 字节不变，便于后续人工或 force-recovery 策略决策。
     */
    private DoublewriteRepairSummary validateDoublewritePages(RecoveryRequest request) {
        int detectedOnly = 0;
        if (request.doublewriteScanner() == null) {
            return new DoublewriteRepairSummary(0, detectedOnly, 0);
        }
        for (PageId pageId : request.pagesToRepair()) {
            DoublewriteRecoveryResult result = request.doublewriteScanner().scanPageForValidation(pageId);
            if (result.diagnosticOnly()) {
                detectedOnly++;
            }
        }
        return new DoublewriteRepairSummary(0, detectedOnly, 0);
    }

    /**
     * 任意阶段失败时保持 gate 关闭并记录失败快照。透传发起恢复时的 {@code mode}，避免失败诊断报告把非 NORMAL 模式
     * 误记为 NORMAL。报告里的 LSN/计数填 0：失败可能发生在尚未读出 checkpoint 或扫描 redo 之前，无可信进度可报。
     */
    private void failClosed(RecoveryRequest request, Throwable error) {
        gate.failClosed(error);
        state = RecoveryState.FAILED;
        lastError = error;
        lastReport = RecoveryReport.failed(request.mode(), request.skipPolicy().skippedSpaces());
    }

    /**
     * 尝试记录失败阶段。progress journal 自身也可能因为诊断文件 IO 失败而抛异常，此时仍必须继续
     * {@link #failClosed(RecoveryMode, Throwable)}，否则 recovery gate 会停在不确定状态；二次失败作为
     * suppressed cause 保留给调用方诊断。
     */
    private static void recordFailureProgress(RecoveryStageTracker tracker, RuntimeException cause) {
        try {
            tracker.fail(cause);
        } catch (RuntimeException progressFailure) {
            cause.addSuppressed(progressFailure);
        }
    }

    private static String skipDetail(String label, int count, RecoverySkipPolicy skipPolicy) {
        if (count == 0) {
            return "";
        }
        return label + "=" + count + " skippedSpaces=" + skipPolicy.describeSkippedSpaces();
    }

    private record DoublewriteRepairSummary(int repairedPageCount, int detectedOnlyPageCount, int skippedPageCount) {
    }

    /**
     * 单次 recover 调用内的阶段追踪器。它把“阶段进入/完成/失败”的记录与原有 completedStages 列表保持一致；
     * 完成后清空 currentStage，避免阶段之间的异常被误记到已经完成的阶段。
     */
    private static final class RecoveryStageTracker {
        private final RecoveryMode mode;
        private final RecoveryProgressJournal journal;
        private final List<RecoveryStageName> completedStages;
        private RecoveryStageName currentStage;

        private RecoveryStageTracker(RecoveryMode mode, RecoveryProgressJournal journal,
                                     List<RecoveryStageName> completedStages) {
            this.mode = mode;
            this.journal = journal;
            this.completedStages = completedStages;
        }

        private void begin(RecoveryStageName stageName) {
            currentStage = stageName;
            journal.stageStarted(mode, stageName);
        }

        private void complete(RecoveryState state, Lsn recoveredToLsn) {
            complete(state, recoveredToLsn, "");
        }

        private void complete(RecoveryState state, Lsn recoveredToLsn, String detail) {
            if (currentStage == null) {
                throw new DatabaseValidationException("recovery stage completion without active stage");
            }
            completedStages.add(currentStage);
            journal.stageCompleted(mode, currentStage, state, recoveredToLsn, detail);
            currentStage = null;
        }

        private void fail(Throwable cause) {
            if (currentStage != null) {
                journal.stageFailed(mode, currentStage, cause);
            }
        }

        private List<RecoveryStageName> completedStages() {
            return completedStages;
        }
    }
}
