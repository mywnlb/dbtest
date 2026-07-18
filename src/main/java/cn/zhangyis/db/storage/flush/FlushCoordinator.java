package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.DirtyPageCandidate;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessLease;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteStrategy;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.redo.RedoLogManager;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;

/**
 * F1 同步 Flush 协调器。它只编排 dirty page snapshot、redo durable gate、doublewrite、data file write 和
 * Buffer Pool clean/keep-dirty 回调，不维护 Buffer Pool 内部链表，也不直接操作 FileChannel。
 */
public final class FlushCoordinator {

    /**
     * 本对象持有的 {@code bufferPool} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final BufferPool bufferPool;
    /**
     * 本对象持有的 {@code pageStore} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PageStore pageStore;
    /**
     * 本对象持有的 {@code redo} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final RedoLogManager redo;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;
    /**
     * 本对象持有的 {@code doublewrite} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DoublewriteStrategy doublewrite;
    /**
     * 构造时冻结的 {@code redoWaitTimeout} 时间边界；不得为负，零仅表示立即检查，等待路径依赖该值保证不会无界阻塞。
     */
    private final Duration redoWaitTimeout;
    /** 与 MTR/truncate 共用的 operation lease；flush 持共享 lease 覆盖 snapshot 到 data-file force。 */
    private final TablespaceAccessController accessController;

    /**
     * 创建 {@code FlushCoordinator}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param bufferPool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param doublewrite 由组合根提供的 {@code DoublewriteStrategy} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redoWaitTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     */
    public FlushCoordinator(BufferPool bufferPool, PageStore pageStore, RedoLogManager redo, PageSize pageSize,
                            DoublewriteStrategy doublewrite, Duration redoWaitTimeout) {
        this(bufferPool, pageStore, redo, pageSize, doublewrite, redoWaitTimeout,
                new TablespaceAccessController());
    }

    /**
     * 创建与 lifecycle 服务共享准入控制器的 flush 协调器。截断线程持 X 时可在同线程重入 S 完成 marker flush；
     * 其它 flusher 会在 X 外等待，不能把尾页 snapshot 跨越物理 truncate。
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
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param doublewrite 由组合根提供的 {@code DoublewriteStrategy} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redoWaitTimeout 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @param accessController 由组合根提供的 {@code TablespaceAccessController} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FlushCoordinator(BufferPool bufferPool, PageStore pageStore, RedoLogManager redo, PageSize pageSize,
                            DoublewriteStrategy doublewrite, Duration redoWaitTimeout,
                            TablespaceAccessController accessController) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (bufferPool == null || pageStore == null || redo == null || pageSize == null
                || doublewrite == null || redoWaitTimeout == null || accessController == null) {
            throw new DatabaseValidationException("flush coordinator dependencies must not be null");
        }
        if (redoWaitTimeout.isNegative()) {
            throw new DatabaseValidationException("redo wait timeout must not be negative: " + redoWaitTimeout);
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.bufferPool = bufferPool;
        this.pageStore = pageStore;
        this.redo = redo;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.pageSize = pageSize;
        this.doublewrite = doublewrite;
        this.redoWaitTimeout = redoWaitTimeout;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.accessController = accessController;
    }

    /**
     * 按 Buffer Pool dirty view 选择 oldest <= targetLsn 的页，逐页同步 flush。
     *
     * @param targetLsn flush list 目标 LSN。
     * @param maxPages 最多刷页数。
     * @return 每个候选页的结果。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public List<FlushResult> flushList(Lsn targetLsn, int maxPages) {
        if (targetLsn == null) {
            throw new DatabaseValidationException("target LSN must not be null");
        }
        if (maxPages < 0) {
            throw new DatabaseValidationException("max pages must not be negative: " + maxPages);
        }
        return flushBatch(new FlushBatchRequest(FlushBatchSource.FLUSH_LIST, targetLsn, maxPages));
    }

    /**
     * 批量执行 WAL-safe 刷脏。候选固定、redo 等待、doublewrite 和 data-file force 分阶段执行，
     * 且物理 IO 期间不持有 Buffer Pool 内部锁。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param request 候选来源、目标 LSN 和页数上限。
     * @return 每个候选页的结果，顺序与候选 snapshot 顺序一致。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public List<FlushResult> flushBatch(FlushBatchRequest request) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        if (request == null) {
            throw new DatabaseValidationException("flush batch request must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        List<DirtyPageCandidate> candidates = request.source() == FlushBatchSource.LRU
                ? bufferPool.lruDirtyPageCandidates(request.maxPages())
                : bufferPool.dirtyPageCandidates(request.targetLsn(), request.maxPages());
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        if (candidates.isEmpty()) {
            return List.of();
        }
        List<TablespaceAccessLease> leases = acquireBatchLeases(candidates);
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        try {
            List<FlushPageSnapshot> snapshots = new ArrayList<>();
            List<FlushResult> results = new ArrayList<>();
            for (DirtyPageCandidate candidate : candidates) {
                Optional<FlushPageSnapshot> maybe = bufferPool.snapshotForFlush(candidate.pageId());
                if (maybe.isEmpty()) {
                    results.add(FlushResult.ok(candidate.pageId(), candidate.newestModificationLsn(),
                            FlushResultStatus.SKIPPED_NOT_DIRTY));
                    continue;
                }
                FlushPageSnapshot snapshot = maybe.orElseThrow();
                if (snapshot.pageLsn().value() > redo.flushedToDiskLsn().value()
                        && !redo.waitFlushed(snapshot.pageLsn(), redoWaitTimeout)) {
                    bufferPool.failFlush(snapshot.pageId());
                    results.add(FlushResult.ok(snapshot.pageId(), snapshot.pageLsn(),
                            FlushResultStatus.SKIPPED_REDO_NOT_DURABLE));
                    continue;
                }
                byte[] image = snapshot.pageImage();
                PageImageChecksum.stamp(image, pageSize);
                snapshots.add(new FlushPageSnapshot(snapshot.pageId(), snapshot.pageLsn(),
                        snapshot.dirtyVersion(), snapshot.generation(), image));
            }
            if (snapshots.isEmpty()) {
                return List.copyOf(results);
            }
            List<FlushPageSnapshot> dataWritten = new ArrayList<>();
            try {
                doublewrite.beforeDataFileWriteBatch(request.source(), List.copyOf(snapshots));
                Map<SpaceId, List<FlushPageSnapshot>> bySpace = groupBySpace(snapshots);
                for (Map.Entry<SpaceId, List<FlushPageSnapshot>> entry : bySpace.entrySet()) {
                    for (FlushPageSnapshot snapshot : entry.getValue()) {
                        pageStore.writePage(snapshot.pageId(), ByteBuffer.wrap(snapshot.pageImage()));
                        dataWritten.add(snapshot);
                    }
                    pageStore.force(entry.getKey());
                }
                doublewrite.afterDataFileWriteBatch(request.source(), List.copyOf(dataWritten));
                for (FlushPageSnapshot snapshot : snapshots) {
                    boolean clean = bufferPool.completeFlush(snapshot);
                    results.add(FlushResult.ok(snapshot.pageId(), snapshot.pageLsn(),
                            clean ? FlushResultStatus.CLEAN : FlushResultStatus.KEPT_DIRTY));
                }
            } catch (DatabaseRuntimeException failure) {
                doublewrite.abortDataFileWriteBatch(request.source(), List.copyOf(snapshots));
                Set<PageId> writtenIds = dataWritten.stream().map(FlushPageSnapshot::pageId).collect(java.util.stream.Collectors.toSet());
                for (FlushPageSnapshot snapshot : snapshots) {
                    if (!writtenIds.contains(snapshot.pageId())) {
                        bufferPool.failFlush(snapshot.pageId());
                    }
                }
                throw failure;
            }
            return List.copyOf(results);
        } finally {
            releaseBatchLeases(leases);
        }
    }

    /**
     * 按脏页刷盘与 checkpoint并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param candidates 参与 {@code acquireBatchLeases} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code acquireBatchLeases} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     */
    private List<TablespaceAccessLease> acquireBatchLeases(List<DirtyPageCandidate> candidates) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        Set<SpaceId> spaces = new TreeSet<>(java.util.Comparator.comparingLong(SpaceId::value));
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        for (DirtyPageCandidate candidate : candidates) {
            spaces.add(candidate.pageId().spaceId());
        }
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        List<TablespaceAccessLease> leases = new ArrayList<>();
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        try {
            for (SpaceId space : spaces) {
                leases.add(accessController.acquireShared(space));
            }
            return leases;
        } catch (DatabaseRuntimeException failure) {
            releaseBatchLeases(leases);
            throw failure;
        }
    }

    /**
     * 释放本方法拥有的脏页刷盘与 checkpoint资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param leases 参与 {@code releaseBatchLeases} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    private void releaseBatchLeases(List<TablespaceAccessLease> leases) {
        for (int i = leases.size() - 1; i >= 0; i--) {
            leases.get(i).close();
        }
    }

    /**
     * 根据调用参数创建或转换 {@code groupBySpace} 返回的 {@code Map<SpaceId, List<FlushPageSnapshot>>}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code groupBySpace} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     */
    private static Map<SpaceId, List<FlushPageSnapshot>> groupBySpace(List<FlushPageSnapshot> snapshots) {
        Map<SpaceId, List<FlushPageSnapshot>> grouped = new TreeMap<>(java.util.Comparator.comparingLong(SpaceId::value));
        for (FlushPageSnapshot snapshot : snapshots) {
            grouped.computeIfAbsent(snapshot.pageId().spaceId(), ignored -> new ArrayList<>()).add(snapshot);
        }
        return grouped;
    }

    /**
     * 同步刷一个指定页。若页当前不脏或仍被 fixed，则返回 SKIPPED_NOT_DIRTY。
     *
     * @param pageId 目标页。
     * @return flush 结果。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FlushResult singlePageFlush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        return flushPage(pageId, Lsn.of(0));
    }

    private FlushResult flushPage(PageId pageId, Lsn observedPageLsn) {
        try (TablespaceAccessLease ignored = accessController.acquireShared(pageId.spaceId())) {
            return flushPageUnderLease(pageId, observedPageLsn);
        }
    }

    /** 调用方已持目标空间共享 operation lease；整个 snapshot/doublewrite/data force/clean 回调不可跨越 truncate X。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param observedPageLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @return {@code flushPageUnderLease} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private FlushResult flushPageUnderLease(PageId pageId, Lsn observedPageLsn) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        Optional<FlushPageSnapshot> maybeSnapshot = bufferPool.snapshotForFlush(pageId);
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        if (maybeSnapshot.isEmpty()) {
            return FlushResult.ok(pageId, observedPageLsn, FlushResultStatus.SKIPPED_NOT_DIRTY);
        }
        FlushPageSnapshot snapshot = maybeSnapshot.orElseThrow();
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        if (snapshot.pageLsn().value() > redo.flushedToDiskLsn().value()
                && !redo.waitFlushed(snapshot.pageLsn(), redoWaitTimeout)) {
            // 已 snapshot（帧已 DIRTY→FLUSHING）但 WAL gate 未放行：不能写盘，必须把帧从 FLUSHING 退回 DIRTY，
            // 否则它会被排除出 flush 候选/淘汰、永久卡在 FLUSHING（WAL 未 durable 时 flush 正确跳过，但状态须复位）。
            bufferPool.failFlush(pageId);
            return FlushResult.ok(pageId, snapshot.pageLsn(), FlushResultStatus.SKIPPED_REDO_NOT_DURABLE);
        }
        boolean doublewritePrepared = false;
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        try {
            byte[] image = snapshot.pageImage();
            PageImageChecksum.stamp(image, pageSize);
            FlushPageSnapshot stamped = new FlushPageSnapshot(snapshot.pageId(), snapshot.pageLsn(),
                    snapshot.dirtyVersion(), snapshot.generation(), image);
            doublewrite.beforeDataFileWriteBatch(FlushBatchSource.LRU, List.of(stamped));
            doublewritePrepared = true;
            pageStore.writePage(stamped.pageId(), ByteBuffer.wrap(stamped.pageImage()));
            pageStore.force(stamped.pageId().spaceId());
            boolean clean = bufferPool.completeFlush(stamped);
            doublewrite.afterDataFileWriteBatch(FlushBatchSource.LRU, List.of(stamped));
            return FlushResult.ok(pageId, snapshot.pageLsn(),
                    clean ? FlushResultStatus.CLEAN : FlushResultStatus.KEPT_DIRTY);
        } catch (DatabaseRuntimeException e) {
            if (doublewritePrepared) {
                doublewrite.abortDataFileWriteBatch(FlushBatchSource.LRU, List.of(snapshot));
            }
            bufferPool.failFlush(pageId);
            return FlushResult.failed(pageId, snapshot.pageLsn(), e);
        }
    }
}
