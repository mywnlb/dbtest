package cn.zhangyis.db.storage.api.undotruncate;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.meta.Tablespace;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessLease;
import cn.zhangyis.db.storage.fil.meta.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.fsp.segment.SegmentInodeRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.undo.UndoTablespaceFspRebuilder;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.redo.RedoLogManager;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * UNDO 单文件表空间 crash-safe 截断编排器。
 *
 * <p>不可逆边界是 page-0 TRUNCATING marker 的 redo durable：在此之前任何失败不碰物理文件；在此之后失败
 * 保持 marker，恢复或下一次调用按同 epoch/target 续作。完整顺序为：X operation lease → 拒绝 active slot 并排空
 * cached segment → 空 inode 校验 →
 * marker MTR + redo fsync → 全局 flush/checkpoint barrier → Buffer Pool drain/invalidate → 物理 truncate+force →
 * page0/page2 空基线重建并持久化 → 最终状态持久化 → Registry 缩小快照发布。
 *
 * <p>与 MySQL/InnoDB 的简化差异：目标固定为新建时 initial size；不选择轮换 undo space，purge scheduler 作为
 * 独立协作者只调用本服务的稳定 attempt 端口；不迁移旧 UNDO（无生命周期头的文件明确拒绝）。
 */
public final class UndoTablespaceTruncationService implements UndoTruncationAttemptTarget {

    /**
     * 本对象持有的 {@code bufferPool} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final BufferPool bufferPool;
    /**
     * 本对象持有的 {@code pageStore} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PageStore pageStore;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;
    /**
     * 本对象持有的 {@code registry} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TablespaceRegistry registry;
    /**
     * 本对象持有的 {@code accessController} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TablespaceAccessController accessController;
    /**
     * 本对象持有的 {@code mtrManager} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final MiniTransactionManager mtrManager;
    /**
     * 本对象持有的 {@code redo} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final RedoLogManager redo;
    /**
     * 本对象持有的 {@code flushService} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final FlushService flushService;
    /**
     * 构造时冻结的 {@code waitTimeout} 时间边界；不得为负，零仅表示立即检查，等待路径依赖该值保证不会无界阻塞。
     */
    private final Duration waitTimeout;
    /**
     * 本次事务链路持有的 {@code faultInjector} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
     */
    private final UndoTruncationFaultInjector faultInjector;
    /**
     * 本对象持有的 {@code headerRepository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SpaceHeaderRepository headerRepository;
    /**
     * 本对象持有的 {@code inodeRepository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SegmentInodeRepository inodeRepository;
    /**
     * 本对象持有的 {@code rebuilder} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final UndoTablespaceFspRebuilder rebuilder;
    /** 负责 marker 前验证空 history/排空 cache/free owner，以及物理重建时恢复 page3 v4 空目录。 */
    private final UndoReusableSegmentTruncationCoordinator cachedSegmentCoordinator;

    /**
     * 创建 {@code UndoTablespaceTruncationService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param bufferPool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param registry 由组合根提供的 {@code TablespaceRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param accessController 由组合根提供的 {@code TablespaceAccessController} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param mtrManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param flushService 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param waitTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @param faultInjector 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param cachedSegmentCoordinator 由组合根提供的 {@code UndoReusableSegmentTruncationCoordinator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoTablespaceTruncationService(
            BufferPool bufferPool,
            PageStore pageStore,
            PageSize pageSize,
            TablespaceRegistry registry,
            TablespaceAccessController accessController,
            MiniTransactionManager mtrManager,
            FlushService flushService,
            Duration waitTimeout,
            UndoTruncationFaultInjector faultInjector,
            UndoReusableSegmentTruncationCoordinator cachedSegmentCoordinator) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (bufferPool == null || pageStore == null || pageSize == null || registry == null
                || accessController == null || mtrManager == null || flushService == null
                || waitTimeout == null || faultInjector == null || cachedSegmentCoordinator == null) {
            throw new DatabaseValidationException("undo truncation dependencies must not be null");
        }
        if (waitTimeout.isZero() || waitTimeout.isNegative()) {
            throw new DatabaseValidationException("undo truncation wait timeout must be positive");
        }
        this.bufferPool = bufferPool;
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.registry = registry;
        this.accessController = accessController;
        this.mtrManager = mtrManager;
        this.redo = mtrManager.redoLogManager();
        this.flushService = flushService;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.waitTimeout = waitTimeout;
        this.faultInjector = faultInjector;
        this.headerRepository = new SpaceHeaderRepository(bufferPool);
        this.inodeRepository = new SegmentInodeRepository(bufferPool, pageSize);
        this.rebuilder = new UndoTablespaceFspRebuilder(bufferPool, pageSize);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.cachedSegmentCoordinator = cachedSegmentCoordinator;
    }

    /**
     * 为后台 purge maintenance 零等待尝试自动截断当前单 UNDO 表空间。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验候选阈值并立即尝试 lifecycle X lease；普通访问或公平队列忙时不等待，返回 access deferred。</li>
     *     <li>在 X lease 内读取 page0 持久 lifecycle 和物理文件大小；稳定空间按 persisted initial size 计算增长，
     *     未达 extent 门槛直接跳过，已有 TRUNCATING 证据则忽略门槛继续续作。</li>
     *     <li>对稳定候选非阻塞取得 reuse drain gate；history、active slot 或 transition busy 均在 marker/物理副作用前
     *     返回具体 deferred，cache/free owner 则按既有有界批次真正回收。</li>
     *     <li>复用严格 {@link #truncate(SpaceId, TablespaceState)} 协议完成 inode 复核、marker、flush、物理收缩与发布；
     *     嵌套 X lease 是同线程可重入，异常继续保留原有 crash/fail-stop 边界。</li>
     * </ol>
     *
     * @param spaceId 配置的系统 UNDO 表空间稳定身份；必须已打开并具有 lifecycle header
     * @param minReclaimableExtents 相对持久 initial size 的最小增长 extent 数；必须严格为正
     * @return 本轮完成、阈值跳过或无物理副作用 deferred 的不可变结果
     * @throws DatabaseValidationException spaceId 为空或 extent 阈值非法时抛出，调用方应修正配置
     * @throws UndoTablespaceTruncationException lifecycle、owner、WAL、flush 或物理 IO 无法安全继续时抛出；
     *         调度器不得把这类错误降级为 deferred
     */
    @Override
    public UndoTruncationAttemptResult tryTruncate(SpaceId spaceId, int minReclaimableExtents) {
        // 1、自动维护不能排队阻塞普通 IO；配置错误必须在访问锁表前失败。
        validateRequest(spaceId, TablespaceState.ACTIVE);
        if (minReclaimableExtents <= 0) {
            throw new DatabaseValidationException(
                    "undo truncate min reclaimable extents must be positive: " + minReclaimableExtents);
        }
        var optionalLease = accessController.tryAcquireExclusive(spaceId);
        if (optionalLease.isEmpty()) {
            return UndoTruncationAttemptResult.incomplete(
                    UndoTruncationAttemptStatus.DEFERRED_ACCESS_BUSY, 0L);
        }

        try (TablespaceAccessLease ignored = optionalLease.orElseThrow()) {
            // 2、阈值只信任持久 initial size；配置中的 fresh size 不能覆盖 existing 文件创建时的真相。
            Tablespace runtime = registry.requireForRecovery(spaceId).tablespace();
            if (runtime.type() != TablespaceType.UNDO) {
                throw new UndoTablespaceTruncationException(
                        "only UNDO tablespace can use automatic undo truncation: " + spaceId.value());
            }
            ReadState read = readState(spaceId);
            TablespaceLifecycleHeader lifecycle = read.lifecycle().orElseThrow(() ->
                    new UndoTablespaceTruncationException(
                            "legacy UNDO tablespace has no lifecycle header; offline migration is required: "
                                    + spaceId.value()));
            validateTarget(lifecycle);
            long physicalPages = pageStore.currentSizeInPages(spaceId).value();
            long initialPages = lifecycle.initialSizeInPages().value();
            if (physicalPages < initialPages) {
                throw new UndoTablespaceTruncationException(
                        "UNDO file is smaller than persisted initial size: current=" + physicalPages
                                + ", initial=" + initialPages);
            }
            long reclaimablePages = physicalPages - initialPages;
            long thresholdPages;
            try {
                thresholdPages = Math.multiplyExact((long) minReclaimableExtents, pageSize.pagesPerExtent());
            } catch (ArithmeticException overflow) {
                throw new DatabaseValidationException("undo truncate extent threshold overflows page count", overflow);
            }
            boolean continuing = lifecycle.state() == TablespaceState.TRUNCATING
                    || runtime.state() == TablespaceState.TRUNCATING;
            if (!continuing && reclaimablePages < thresholdPages) {
                return UndoTruncationAttemptResult.incomplete(
                        UndoTruncationAttemptStatus.BELOW_THRESHOLD, reclaimablePages);
            }

            TablespaceState finishState = lifecycle.state() == TablespaceState.TRUNCATING
                    ? lifecycle.finishState() : lifecycle.state();
            if (!continuing) {
                ensureStableSourceState(finishState);
                // 3、normal deferral 与真实存储错误分流；只有 DRAINED 才能进入 marker 协议。
                UndoReusableSegmentDrainStatus drain = cachedSegmentCoordinator.tryDrainStableSpace(spaceId);
                UndoTruncationAttemptStatus deferred = switch (drain) {
                    case DRAINED -> null;
                    case DEFERRED_HISTORY -> UndoTruncationAttemptStatus.DEFERRED_HISTORY;
                    case DEFERRED_ACTIVE_SLOTS -> UndoTruncationAttemptStatus.DEFERRED_ACTIVE_SLOTS;
                    case DEFERRED_REUSE_BUSY -> UndoTruncationAttemptStatus.DEFERRED_REUSE_BUSY;
                };
                if (deferred != null) {
                    return UndoTruncationAttemptResult.incomplete(deferred, reclaimablePages);
                }
            }

            // 4、严格入口在同线程重入 X lease并重新校验全部物理证据；完成结果携带 durable epoch。
            UndoTablespaceTruncationResult completion = truncate(spaceId, finishState);
            return UndoTruncationAttemptResult.completed(reclaimablePages, completion);
        }
    }

    /**
     * 新建截断 marker 或续作已有 TRUNCATING marker。整个流程持每空间独占 lease；内部维护 MTR/flush 在同线程
     * 重入共享 lease，外部普通访问必须等最终状态发布后重新检查 Registry。
     *
     * @param spaceId 显式配置且已打开的 UNDO 表空间。
     * @param finishState 完成后 ACTIVE 或 INACTIVE；续作时必须与 marker 中记录的一致。
     * @return 稳定完成结果。
     * @throws UndoTablespaceTruncationException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public UndoTablespaceTruncationResult truncate(SpaceId spaceId, TablespaceState finishState) {
        validateRequest(spaceId, finishState);
        try (TablespaceAccessLease ignored = accessController.acquireExclusive(spaceId)) {
            Tablespace runtime = registry.requireForRecovery(spaceId).tablespace();
            if (runtime.type() != TablespaceType.UNDO) {
                throw new UndoTablespaceTruncationException("only UNDO tablespace can use undo truncation: "
                        + spaceId.value() + " type=" + runtime.type());
            }
            ReadState read = readState(spaceId);
            TablespaceLifecycleHeader lifecycle = read.lifecycle().orElseThrow(() ->
                    new UndoTablespaceTruncationException("legacy UNDO tablespace has no lifecycle header; "
                            + "offline migration is required: " + spaceId.value()));
            validateTarget(lifecycle);

            // crash 位于最终 page0 状态 durable 与 Registry 发布之间：磁盘已稳定，直接重发运行时快照即可。
            if (lifecycle.state() != TablespaceState.TRUNCATING
                    && runtime.state() == TablespaceState.TRUNCATING
                    && pageStore.currentSizeInPages(spaceId).equals(lifecycle.targetSizeInPages())) {
                publish(runtime, lifecycle.state(), lifecycle.targetSizeInPages(), read.header().freeLimitPageNo());
                return result(spaceId, lifecycle, read.pageLsn());
            }

            // 已经在初始大小的稳定空间重复调用是幂等 no-op，不无意义推进 epoch。
            if (lifecycle.state() != TablespaceState.TRUNCATING
                    && pageStore.currentSizeInPages(spaceId).equals(lifecycle.initialSizeInPages())) {
                if (lifecycle.state() != finishState) {
                    throw new UndoTablespaceTruncationException("stable UNDO state differs from requested finish state: "
                            + lifecycle.state() + " vs " + finishState);
                }
                return result(spaceId, lifecycle, read.pageLsn());
            }

            TablespaceLifecycleHeader marker;
            Lsn markerLsn;
            if (lifecycle.state() == TablespaceState.TRUNCATING) {
                if (lifecycle.finishState() != finishState) {
                    throw new UndoTablespaceTruncationException("TRUNCATING marker finish state mismatch: marker="
                            + lifecycle.finishState() + ", requested=" + finishState);
                }
                marker = lifecycle;
                markerLsn = read.pageLsn();
                if (runtime.state() != TablespaceState.TRUNCATING) {
                    runtime = publish(runtime, TablespaceState.TRUNCATING,
                            pageStore.currentSizeInPages(spaceId), read.header().freeLimitPageNo());
                }
                // marker 必须晚于 history/active/cache owner 清空；恢复期内存投影尚未重建，只依据 page3 持久证据守门。
                cachedSegmentCoordinator.requirePersistentEmpty(spaceId);
                ensureNoAllocatedInodes(spaceId);
            } else {
                ensureStableSourceState(lifecycle.state());
                // 持 X 时只尝试非阻塞 cache gate；忙则抛出并释放 X，由调用方重试，绝不与 finalizer 形成等待环。
                cachedSegmentCoordinator.drainStableSpace(spaceId);
                ensureNoAllocatedInodes(spaceId);
                marker = new TablespaceLifecycleHeader(TablespaceState.TRUNCATING,
                        lifecycle.initialSizeInPages(), lifecycle.truncateEpoch() + 1,
                        lifecycle.initialSizeInPages(), finishState);
                markerLsn = writeLifecycle(spaceId, marker);
                ensureRedoDurable(markerLsn, "truncate marker");
                runtime = publish(runtime, TablespaceState.TRUNCATING,
                        pageStore.currentSizeInPages(spaceId), read.header().freeLimitPageNo());
                faultInjector.after(UndoTruncationPhase.AFTER_MARKER_DURABLE);
            }

            // fresh/retry in-process 都必须先覆盖 marker。若新进程 redo 恢复尚未发布该边界，应由 recovery 编排先完成 replay。
            if (markerLsn.value() > redo.currentLsn().value()) {
                throw new UndoTablespaceTruncationException("redo recovery boundary has not reached truncate marker: marker="
                        + markerLsn.value() + ", current=" + redo.currentLsn().value());
            }
            flushService.flushThrough(markerLsn, waitTimeout);
            bufferPool.invalidateTablespace(spaceId, waitTimeout);
            faultInjector.after(UndoTruncationPhase.AFTER_BUFFER_INVALIDATION);

            PageNo physicalSize = pageStore.currentSizeInPages(spaceId);
            if (physicalSize.value() < marker.targetSizeInPages().value()) {
                throw new UndoTablespaceTruncationException("UNDO file is smaller than durable truncate target: current="
                        + physicalSize.value() + ", target=" + marker.targetSizeInPages().value());
            }
            if (physicalSize.value() > marker.targetSizeInPages().value()) {
                pageStore.truncate(spaceId, marker.targetSizeInPages());
            }
            faultInjector.after(UndoTruncationPhase.AFTER_PHYSICAL_TRUNCATE);

            // 重建使用截断前可读 header 保留 type flags/server version；page0/page2 全页重初始化清除所有旧账本。
            Lsn rebuildLsn = rebuild(read.header(), marker);
            ensureRedoDurable(rebuildLsn, "FSP rebuild");
            flushService.flushThrough(rebuildLsn, waitTimeout);
            faultInjector.after(UndoTruncationPhase.AFTER_REBUILD_DURABLE);

            TablespaceLifecycleHeader completed = new TablespaceLifecycleHeader(finishState,
                    marker.initialSizeInPages(), marker.truncateEpoch(), marker.targetSizeInPages(), finishState);
            Lsn finalLsn = writeLifecycle(spaceId, completed);
            ensureRedoDurable(finalLsn, "final lifecycle state");
            flushService.flushThrough(finalLsn, waitTimeout);
            pageStore.force(spaceId);
            faultInjector.after(UndoTruncationPhase.AFTER_FINAL_STATE_DURABLE);

            publish(runtime, finishState, marker.targetSizeInPages(), PageNo.of(0));
            return new UndoTablespaceTruncationResult(spaceId, marker.truncateEpoch(),
                    marker.targetSizeInPages(), markerLsn, finishState);
        }
    }

    /**
     * 定位并读取存储引擎稳定 API领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code readState} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private ReadState readState(SpaceId spaceId) {
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            SpaceHeaderSnapshot header = headerRepository.read(mtr, spaceId);
            Optional<TablespaceLifecycleHeader> lifecycle = headerRepository.readLifecycle(mtr, spaceId);
            PageGuard page0 = mtr.getPage(bufferPool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.SHARED);
            Lsn pageLsn = PageEnvelope.readPageLsn(page0);
            mtrManager.commit(mtr);
            return new ReadState(header, lifecycle, pageLsn);
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr);
            throw failure;
        }
    }

    /**
     * 校验 {@code ensureNoAllocatedInodes} 涉及的存储引擎稳定 API结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @throws UndoTablespaceNotEmptyException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private void ensureNoAllocatedInodes(SpaceId spaceId) {
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            boolean allocated = inodeRepository.hasAllocatedSlots(mtr, spaceId);
            mtrManager.commit(mtr);
            if (allocated) {
                throw new UndoTablespaceNotEmptyException(
                        "UNDO tablespace still owns allocated inode slots: " + spaceId.value());
            }
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr);
            throw failure;
        }
    }

    /**
     * 校验输入与当前状态后修改存储引擎稳定 API领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param lifecycle 表空间文件或 segment 的稳定身份与生命周期快照；不得为 {@code null}，必须与已打开文件和当前 generation 一致
     * @return {@code writeLifecycle} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    private Lsn writeLifecycle(SpaceId spaceId, TablespaceLifecycleHeader lifecycle) {
        MiniTransaction mtr = mtrManager.begin(
                mtrManager.budgetFor(RedoBudgetPurpose.UNDO_TRUNCATE_LIFECYCLE));
        try {
            headerRepository.writeLifecycle(mtr, spaceId, lifecycle);
            return mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr);
            throw failure;
        }
    }

    private Lsn rebuild(SpaceHeaderSnapshot previous, TablespaceLifecycleHeader marker) {
        MiniTransaction mtr = mtrManager.begin(
                mtrManager.budgetFor(RedoBudgetPurpose.UNDO_TRUNCATE_REBUILD));
        try {
            rebuilder.rebuild(mtr, previous, marker);
            cachedSegmentCoordinator.rebuildHeader(mtr, previous.spaceId());
            return mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackIfActive(mtr);
            throw failure;
        }
    }

    private void ensureRedoDurable(Lsn target, String phase) {
        Lsn durable = redo.flush();
        if (durable.value() < target.value()) {
            throw new UndoTablespaceTruncationException(phase + " redo is not durable: target="
                    + target.value() + ", durable=" + durable.value());
        }
    }

    /** 发布缩小后的新 metadata 快照，不调用 Tablespace.publishSize（该通用 API 有只增不减不变量）。 */
    private Tablespace publish(Tablespace previous, TablespaceState state, PageNo currentSize, PageNo freeLimit) {
        TablespaceMetadata metadata = new TablespaceMetadata(previous.spaceId(), previous.name(), previous.type(),
                previous.pageSize(), state,
                List.of(DataFileDescriptor.single(pageStore.pathOf(previous.spaceId()), PageNo.of(0), currentSize)),
                previous.spaceFlags(), currentSize, freeLimit, previous.spaceVersion() + 1);
        return registry.replace(metadata).tablespace();
    }

    private void validateTarget(TablespaceLifecycleHeader lifecycle) {
        long target = lifecycle.targetSizeInPages().value();
        int pagesPerExtent = pageSize.pagesPerExtent();
        if (target < pagesPerExtent || target % pagesPerExtent != 0) {
            throw new UndoTablespaceTruncationException("UNDO truncate target must be extent-aligned and at least one "
                    + "extent: target=" + target + ", pagesPerExtent=" + pagesPerExtent);
        }
        if (!lifecycle.targetSizeInPages().equals(lifecycle.initialSizeInPages())) {
            throw new UndoTablespaceTruncationException("current slice only truncates to persisted initial size");
        }
    }

    private static void validateRequest(SpaceId spaceId, TablespaceState finishState) {
        if (spaceId == null || finishState == null) {
            throw new DatabaseValidationException("undo truncate space/finish state must not be null");
        }
        if (finishState != TablespaceState.ACTIVE && finishState != TablespaceState.INACTIVE) {
            throw new DatabaseValidationException("undo truncate finish state must be ACTIVE or INACTIVE");
        }
    }

    private static void ensureStableSourceState(TablespaceState state) {
        if (state != TablespaceState.ACTIVE && state != TablespaceState.INACTIVE) {
            throw new UndoTablespaceTruncationException("UNDO tablespace is not in a truncatable state: " + state);
        }
    }

    private void rollbackIfActive(MiniTransaction mtr) {
        if (mtr.state() == MiniTransactionState.ACTIVE) {
            mtrManager.rollbackUncommitted(mtr);
        }
    }

    private static UndoTablespaceTruncationResult result(
            SpaceId spaceId, TablespaceLifecycleHeader lifecycle, Lsn markerLsn) {
        return new UndoTablespaceTruncationResult(spaceId, lifecycle.truncateEpoch(),
                lifecycle.targetSizeInPages(), markerLsn, lifecycle.state());
    }

    /**
     * 封装存储引擎稳定 API中 {@code ReadState} 的槽位、预留或阶段结果；组件在创建时交叉校验，使恢复和释放路径能区分已完成与剩余工作。
     *
     * @param header 本次操作的不可变上下文或权威快照；不得为 {@code null}，其中的版本、owner 与资源边界必须来自同一次调用链
     * @param lifecycle 可选的 {@code lifecycle}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param pageLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     */
    private record ReadState(
            SpaceHeaderSnapshot header,
            Optional<TablespaceLifecycleHeader> lifecycle,
            Lsn pageLsn) {
    }
}
