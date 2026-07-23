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
import cn.zhangyis.db.storage.trx.PurgeSummary;

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

    /**
     * 创建 {@code CrashRecoveryService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param gate 由组合根提供的 {@code RecoveryTrafficGate} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     */
    public CrashRecoveryService(RecoveryTrafficGate gate) {
        this(gate, new RecoveryProgressJournal());
    }

    /**
     * 创建 {@code CrashRecoveryService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param gate 由组合根提供的 {@code RecoveryTrafficGate} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param progressJournal 由组合根提供的 {@code RecoveryProgressJournal} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RecoveryStartupException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
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

                // Change Buffer 是后续 rollback/purge 可能依赖的物理接管证据；必须先证明 header/tree 完整，
                // 才允许事务 inverse 访问任一可能触发发布前 merge 的二级 leaf。
                if (request.changeBufferRecovery() != null) {
                    tracker.begin(RecoveryStageName.CHANGE_BUFFER_RECOVER);
                    request.changeBufferRecovery().validateAfterRedo();
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

                TransactionUndoRecoveryResult undoResult =
                        new TransactionUndoRecoveryResult(0, 0, 0, 0, 0, 0);
                PurgeSummary purgeSummary = new PurgeSummary(0, 0, 0, 0, 0);
                if (request.transactionUndoRecovery() != null) {
                    // 4、恢复 committed history、决议 PREPARED、再回滚 ACTIVE；返回时所有 undo/page latch 已释放。
                    tracker.begin(RecoveryStageName.UNDO_ROLLBACK);
                    RecoveredTransactionSnapshot transactionSnapshot =
                            request.transactionRecoveryContext().snapshot();
                    undoResult = request.transactionUndoRecovery().recoverAfterRedo(
                                    reader.recoveredToLsn(), transactionSnapshot);
                    if (undoResult == null) {
                        throw new DatabaseValidationException("transaction undo recovery result must not be null");
                    }
                    tracker.complete(state, reader.recoveredToLsn());

                    // 5、在独立阶段真实推进恢复出的 history；异常直接阻止 OPEN_TRAFFIC。
                    tracker.begin(RecoveryStageName.RESUME_PURGE);
                    purgeSummary = request.transactionUndoRecovery().resumePurgeAfterRedoWithSummary();
                    if (purgeSummary == null) {
                        throw new DatabaseValidationException("transaction purge recovery summary must not be null");
                    }
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
                        new RecoveryExclusionSummary(
                                request.skipPolicy().administrativeSpaces(),
                                request.skipPolicy().dictionarySpaces(),
                                doublewriteSummary.skippedPageCount(), redoSummary.skippedRecordCount(),
                                skippedReconcileSpaceCount,
                                undoResult.skippedActiveRollbackRecords(),
                                undoResult.skippedPreparedRollbackRecords(),
                                purgeSummary.recoveryUnavailableRecordsSkipped()));
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param tracker 由组合根提供的 {@code RecoveryStageTracker} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code recoverReadOnlyValidate} 调用
     * @param checkpoint redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code recoverReadOnlyValidate} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private RecoveryReport recoverReadOnlyValidate(RecoveryRequest request, RecoveryStageTracker tracker,
                                                   RedoCheckpointLabel checkpoint) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        tracker.begin(RecoveryStageName.DOUBLEWRITE_REPAIR);
        DoublewriteRepairSummary doublewriteSummary = validateDoublewritePages(request);
        tracker.complete(state, Lsn.of(0));

        if (request.transactionRecoveryContext() != null) {
            // 只读诊断仍必须证明 sidecar 覆盖 redo checkpoint；initialize 只构造私有内存表，既不 apply delta，
            // 也不发布 counter/rollback/history 或写文件。
            request.transactionRecoveryContext().initialize(checkpoint.checkpointLsn());
        }
        RedoRecoveryReader reader = new RedoRecoveryReader(request.redoRepository(), checkpoint.checkpointLsn());
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        tracker.begin(RecoveryStageName.REDO_REPLAY);
        List<RedoLogBatch> batches = reader.readBatches();
        if (request.transactionRecoveryContext() != null) {
            request.transactionRecoveryContext().verifyRedoCoverage(reader.recoveredToLsn());
            request.transactionRecoveryContext().validateTransactionDeltas(batches);
        }
        tracker.complete(state, reader.recoveredToLsn());

        if (request.changeBufferRecovery() != null) {
            tracker.begin(RecoveryStageName.CHANGE_BUFFER_RECOVER);
            request.changeBufferRecovery().validateAfterRedo();
            tracker.complete(state, reader.recoveredToLsn());
        }

        tracker.begin(RecoveryStageName.READ_ONLY_DIAGNOSTIC_OPEN);
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        gate.enterReadOnlyDiagnostic();
        state = RecoveryState.READ_ONLY;
        tracker.complete(state, reader.recoveredToLsn());
        RecoveryReport report = new RecoveryReport(request.mode(), state, checkpoint.checkpointLsn(),
                reader.recoveredToLsn(), 0, doublewriteSummary.detectedOnlyPageCount(), 0,
                tracker.completedStages());
        lastReport = report;
        lastError = null;
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return report;
    }

    /**
     * 在 doublewrite/page apply 等任何恢复写发生前，校验 redo-control 与 redo data 的持久格式属于同一代。
     * 本切片明确拒绝旧 RLG1/control v1 和双格式猜测；若 label 与 repository 声明不一致，恢复必须 fail closed，
     * 不能把同一 LSN 按错误的物理编码解释。
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param checkpoint redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @throws RedoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static void validateRedoFormat(RecoveryRequest request, RedoCheckpointLabel checkpoint) {
        int controlFormat = checkpoint.redoFormatVersion();
        int dataFormat = request.redoRepository().formatVersion();
        if (controlFormat != dataFormat) {
            throw new RedoLogFormatException("redo control/data format mismatch: control="
                    + controlFormat + " data=" + dataFormat);
        }
    }

    /** 当前 service 状态。
     *
     * @return {@code state} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public RecoveryState state() {
        serviceLock.lock();
        try {
            return state;
        } finally {
            serviceLock.unlock();
        }
    }

    /** 最近一次成功恢复报告。
     *
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<RecoveryReport> lastReport() {
        serviceLock.lock();
        try {
            return Optional.ofNullable(lastReport);
        } finally {
            serviceLock.unlock();
        }
    }

    /** 最近一次失败根因。
     *
     * @return 最近一次受控操作记录的失败；尚无失败时为空 {@code Optional}，参数容器与返回值均不使用 Java {@code null}
     */
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code reconcileSpaceFiles} 实际完成的资源、绑定、页或槽位数量；未处理任何对象时为零，结果不得超过输入候选数
     */
    private int reconcileSpaceFiles(RecoveryRequest request) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        PageStore pageStore = request.applyContext().pageStore();
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        PageSize pageSize = request.applyContext().pageSize();
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
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
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
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

    /**
     * 校验输入与当前状态后修改崩溃恢复领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code repairDoublewritePages} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private DoublewriteRepairSummary repairDoublewritePages(RecoveryRequest request) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        int repaired = 0;
        int detectedOnly = 0;
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        int skipped = 0;
        if (request.undoTablespaceRecovery() != null) {
            repaired += request.undoTablespaceRecovery().prepareDoublewrite(request.doublewriteScanner());
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
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
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return new DoublewriteRepairSummary(repaired, detectedOnly, skipped);
    }

    /**
     * READ_ONLY_VALIDATE 的 doublewrite 阶段：只调用 scanner 的只读校验入口，统计 full-copy 可修复页和 detect-only
     * 可疑页，但绝不调用 undo participant、PageStore.writePage 或 force。这样诊断报告能暴露 torn-page 风险，同时
     * 保持原 data file 字节不变，便于后续人工或 force-recovery 策略决策。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code validateDoublewritePages} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private DoublewriteRepairSummary validateDoublewritePages(RecoveryRequest request) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        int detectedOnly = 0;
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        if (request.doublewriteScanner() == null) {
            return new DoublewriteRepairSummary(0, detectedOnly, 0);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        for (PageId pageId : request.pagesToRepair()) {
            DoublewriteRecoveryResult result = request.doublewriteScanner().scanPageForValidation(pageId);
            if (result.diagnosticOnly()) {
                detectedOnly++;
            }
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return new DoublewriteRepairSummary(0, detectedOnly, 0);
    }

    /**
     * 任意阶段失败时保持 gate 关闭并记录失败快照。透传发起恢复时的 {@code mode}，避免失败诊断报告把非 NORMAL 模式
     * 误记为 NORMAL。报告里的 LSN/计数填 0：失败可能发生在尚未读出 checkpoint 或扫描 redo 之前，无可信进度可报。
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param error 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
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
     * @param tracker 由组合根提供的 {@code RecoveryStageTracker} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code recordFailureProgress} 调用
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private static void recordFailureProgress(RecoveryStageTracker tracker, RuntimeException cause) {
        try {
            tracker.fail(cause);
        } catch (RuntimeException progressFailure) {
            cause.addSuppressed(progressFailure);
        }
    }

    private static String skipDetail(String label, int count, RecoverySpaceExclusionPolicy skipPolicy) {
        if (count == 0) {
            return "";
        }
        return label + "=" + count + " skippedSpaces=" + skipPolicy.describeSkippedSpaces();
    }

    /**
     * 封装崩溃恢复中 {@code DoublewriteRepairSummary} 的槽位、预留或阶段结果；组件在创建时交叉校验，使恢复和释放路径能区分已完成与剩余工作。
     *
     * @param repairedPageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param detectedOnlyPageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param skippedPageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     */
    private record DoublewriteRepairSummary(int repairedPageCount, int detectedOnlyPageCount, int skippedPageCount) {
    }

    /**
     * 单次 recover 调用内的阶段追踪器。它把“阶段进入/完成/失败”的记录与原有 completedStages 列表保持一致；
     * 完成后清空 currentStage，避免阶段之间的异常被误记到已经完成的阶段。
     */
    private static final class RecoveryStageTracker {
        /**
         * 本对象的权威状态机字段 {@code mode}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
         */
        private final RecoveryMode mode;
        /**
         * 本对象持有的 {@code journal} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
         */
        private final RecoveryProgressJournal journal;
        /**
         * 本对象拥有的 {@code completedStages} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
        private final List<RecoveryStageName> completedStages;
        /**
         * 本对象的权威状态机字段 {@code currentStage}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
         */
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
