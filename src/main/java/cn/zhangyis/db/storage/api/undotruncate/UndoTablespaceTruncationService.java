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
 * <p>与 MySQL/InnoDB 的简化差异：目标固定为新建时 initial size；不选择轮换 undo space，不实现 purge scheduler，
 * 也不迁移旧 UNDO（无生命周期头的文件明确拒绝）。
 */
public final class UndoTablespaceTruncationService {

    private final BufferPool bufferPool;
    private final PageStore pageStore;
    private final PageSize pageSize;
    private final TablespaceRegistry registry;
    private final TablespaceAccessController accessController;
    private final MiniTransactionManager mtrManager;
    private final RedoLogManager redo;
    private final FlushService flushService;
    private final Duration waitTimeout;
    private final UndoTruncationFaultInjector faultInjector;
    private final SpaceHeaderRepository headerRepository;
    private final SegmentInodeRepository inodeRepository;
    private final UndoTablespaceFspRebuilder rebuilder;
    /** 负责 marker 前验证空 history/排空 cache owner，以及物理重建时恢复 page3 v3 空目录。 */
    private final UndoCachedSegmentTruncationCoordinator cachedSegmentCoordinator;

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
            UndoCachedSegmentTruncationCoordinator cachedSegmentCoordinator) {
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
        this.registry = registry;
        this.accessController = accessController;
        this.mtrManager = mtrManager;
        this.redo = mtrManager.redoLogManager();
        this.flushService = flushService;
        this.waitTimeout = waitTimeout;
        this.faultInjector = faultInjector;
        this.headerRepository = new SpaceHeaderRepository(bufferPool);
        this.inodeRepository = new SegmentInodeRepository(bufferPool, pageSize);
        this.rebuilder = new UndoTablespaceFspRebuilder(bufferPool, pageSize);
        this.cachedSegmentCoordinator = cachedSegmentCoordinator;
    }

    /**
     * 新建截断 marker 或续作已有 TRUNCATING marker。整个流程持每空间独占 lease；内部维护 MTR/flush 在同线程
     * 重入共享 lease，外部普通访问必须等最终状态发布后重新检查 Registry。
     *
     * @param spaceId 显式配置且已打开的 UNDO 表空间。
     * @param finishState 完成后 ACTIVE 或 INACTIVE；续作时必须与 marker 中记录的一致。
     * @return 稳定完成结果。
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

    private record ReadState(
            SpaceHeaderSnapshot header,
            Optional<TablespaceLifecycleHeader> lifecycle,
            Lsn pageLsn) {
    }
}
