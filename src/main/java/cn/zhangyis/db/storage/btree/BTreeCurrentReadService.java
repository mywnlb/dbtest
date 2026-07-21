package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.IsolationLevel;
import cn.zhangyis.db.storage.trx.lock.InsertIntentionLockKey;
import cn.zhangyis.db.storage.trx.lock.LockHandle;
import cn.zhangyis.db.storage.trx.lock.LockManager;
import cn.zhangyis.db.storage.trx.lock.LockWaitTimeoutException;
import cn.zhangyis.db.storage.trx.lock.TransactionLockKey;
import cn.zhangyis.db.storage.trx.lock.TransactionLockMode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * B+Tree current-read 协调器。它把物理短 MTR 定位和事务逻辑锁等待拆开：先定位 record/gap 并提交 MTR 释放
 * page latch/buffer fix，再进入 LockManager 等待；锁授予后重新定位校验，避免持旧 RecordRef 继续访问。
 *
 * <p>当前支持 point current-read、unique insert 物理检查，以及批量 range current-read。2.1 起单聚簇
 * {@code ClusteredDmlService} 已调用 point/unique 路径并在 facade commit/rollback 释放锁；SQL/session/executor
 * 与 range DML 仍在上层后续切片完成。
 */
public final class BTreeCurrentReadService {

    /** 短 MTR 工厂；每次定位/重定位各用一个独立 MTR。 */
    private final MiniTransactionManager mtrManager;

    /** B+Tree 物理定位服务；包内 current-read 定位方法不泄露 page 资源。 */
    private final SplitCapableBTreeIndexService btree;

    /** 事务锁真相来源；本服务只构造 key 和调用 acquire。 */
    private final LockManager lockManager;

    /**
     * 创建 {@code BTreeCurrentReadService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param mtrManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param btree 由组合根提供的 {@code SplitCapableBTreeIndexService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param lockManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public BTreeCurrentReadService(MiniTransactionManager mtrManager,
                                   SplitCapableBTreeIndexService btree,
                                   LockManager lockManager) {
        if (mtrManager == null || btree == null || lockManager == null) {
            throw new DatabaseValidationException("current-read collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        this.btree = btree;
        this.lockManager = lockManager;
    }

    /**
     * 对单个 key 执行 current-read 锁定读。命中记录按 FOR_SHARE/FOR_UPDATE 取 REC_S/REC_X；
     * RC miss 不锁 gap，RR miss 按模式取 GAP_S/GAP_X。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验请求并建立整次调用共享的 monotonic 等待期限。</li>
     *     <li>在短 MTR 内定位 record/gap，提交 MTR 后才进入事务锁等待。</li>
     *     <li>授锁后用新短 MTR 重定位；稳定时保留事务锁并返回当前记录。</li>
     *     <li>落点变化时释放本轮锁，以同一剩余期限重试；失败不泄漏本轮资源。</li>
     * </ol>
     *
     * @return 命中时返回授锁后重新定位得到的当前记录；miss 返回 empty。
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code lockPoint} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @throws BTreeCurrentReadRelocationException 索引定位、结构修改或等待后重定位无法保持 B+Tree 不变量时抛出；调用方应释放 Guard 并回滚或重试
     */
    public Optional<BTreeLookupResult> lockPoint(BTreeIndex index, SearchKey key,
                                                 BTreeCurrentReadRequest request,
                                                 BTreeCurrentReadMode mode) {
        // 1、先固定本次 current-read 的绝对预算；重定位重试不能重新领取完整 timeout。
        validatePoint(index, key, request, mode);
        WaitDeadline deadline = WaitDeadline.after(request.lockWaitTimeout());
        for (int attempt = 0; attempt < request.maxRelocationRetries(); attempt++) {
            // 2、短 MTR 只物化锁落点，提交后才允许进入可能阻塞的 LockManager。
            BTreeCurrentReadPosition position = locate(index, key, false);
            if (!position.hit() && usesReadCommittedLocking(request.isolationLevel())) {
                return Optional.empty();
            }
            TransactionLockKey lockKey = position.hit() ? position.recordKey().orElseThrow() : position.gapKey();
            TransactionLockMode lockMode = pointLockMode(position.hit(), mode);
            LockHandle handle = lockManager.acquire(request.owner(), lockKey, lockMode,
                    deadline.remaining("point lock"));
            try {
                // 3、授锁后重新定位；位置稳定时保留事务锁并返回当前版本。
                BTreeCurrentReadPosition relocated = locate(index, key, false);
                if (samePointPosition(position, relocated)) {
                    return relocated.record();
                }
            } catch (RuntimeException e) {
                handle.close();
                throw e;
            }
            // 4、位置变化只释放本轮新锁；下一轮继续消费同一绝对预算。
            handle.close();
        }
        throw new BTreeCurrentReadRelocationException(
                "current-read point relocation exceeded retries for index " + index.indexId());
    }

    /**
     * unique insert 前的 current-read 物理重复检查。若同 key 记录存在，先取 REC_S 并重定位确认后返回 duplicate；
     * 若不存在，则对目标 gap 取 INSERT_INTENTION 并重定位确认后返回 available。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验物理 unique 索引与请求，并建立跨 relocation 的绝对等待期限。</li>
     *     <li>短 MTR 定位记录或插入 gap，释放页资源后申请 REC_S/INSERT_INTENTION。</li>
     *     <li>授锁后重新定位，把稳定的存在性映射为 duplicate/available。</li>
     *     <li>落点变化时释放本轮锁，并在同一剩余期限内重试。</li>
     * </ol>
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code checkUniqueForInsert} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code checkUniqueForInsert} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws BTreeCurrentReadRelocationException 索引定位、结构修改或等待后重定位无法保持 B+Tree 不变量时抛出；调用方应释放 Guard 并回滚或重试
     */
    public BTreeUniqueCheckResult checkUniqueForInsert(BTreeIndex index, SearchKey key,
                                                       BTreeCurrentReadRequest request) {
        // 1、输入校验后固定绝对预算，避免重复检查因 relocation 累加完整等待时长。
        validateUnique(index, key, request);
        WaitDeadline deadline = WaitDeadline.after(request.lockWaitTimeout());
        for (int attempt = 0; attempt < request.maxRelocationRetries(); attempt++) {
            // 2、在短 MTR 内区分现存记录与目标 gap，并在释放页资源后申请逻辑锁。
            BTreeCurrentReadPosition position = locate(index, key, true);
            TransactionLockKey lockKey = position.hit()
                    ? position.recordKey().orElseThrow()
                    : new InsertIntentionLockKey(position.gapKey());
            TransactionLockMode lockMode = position.hit()
                    ? TransactionLockMode.REC_S
                    : TransactionLockMode.INSERT_INTENTION;
            LockHandle handle = lockManager.acquire(request.owner(), lockKey, lockMode,
                    deadline.remaining("unique-check lock"));
            try {
                // 3、授锁后重新定位；稳定时把记录存在性转换为 duplicate/available 结果。
                BTreeCurrentReadPosition relocated = locate(index, key, true);
                if (samePointPosition(position, relocated)) {
                    return relocated.record().map(BTreeUniqueCheckResult::duplicate)
                            .orElseGet(BTreeUniqueCheckResult::available);
                }
            } catch (RuntimeException e) {
                handle.close();
                throw e;
            }
            // 4、不稳定落点不泄漏本轮锁，重试仍受首次调用建立的 deadline 约束。
            handle.close();
        }
        throw new BTreeCurrentReadRelocationException(
                "unique-check relocation exceeded retries for index " + index.indexId());
    }

    /**
     * 对有界范围执行 current-read 锁定读。数据流为：短 MTR 定位 range 当前记录和终止 gap → 释放 page latch/fix →
     * 按隔离级别申请事务锁 → 短 MTR 重扫并校验锁落点。RC 只锁返回记录；RR 使用 next-key 加终止 gap 防幻读。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验范围并建立逐行、terminal gap 与 relocation 共享的绝对等待期限。</li>
     *     <li>短 MTR 物化有序记录和终止 gap；RC/RU 空范围直接返回。</li>
     *     <li>在无 page latch/fix 时逐项授锁，再以短 MTR 重扫校验完整落点。</li>
     *     <li>落点变化或失败时逆序释放本轮锁；重试不得刷新等待预算。</li>
     * </ol>
     *
     * @return 授锁后重新定位得到的当前范围记录，按索引顺序排列。
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param range 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @throws BTreeCurrentReadRelocationException 索引定位、结构修改或等待后重定位无法保持 B+Tree 不变量时抛出；调用方应释放 Guard 并回滚或重试
     */
    public List<BTreeLookupResult> lockRange(BTreeIndex index, BTreeScanRange range,
                                             BTreeCurrentReadRequest request,
                                             BTreeCurrentReadMode mode) {
        // 1、在任何定位或锁等待前建立一次 monotonic deadline，整段范围读取共享该预算。
        validateRange(index, range, request, mode);
        WaitDeadline deadline = WaitDeadline.after(request.lockWaitTimeout());
        for (int attempt = 0; attempt < request.maxRelocationRetries(); attempt++) {
            // 2、短 MTR 物化有序记录与 terminal gap；RC/RU 空范围不产生 gap 锁。
            BTreeCurrentReadRangePosition position = locateRange(index, range);
            if (position.records().isEmpty()
                    && usesReadCommittedLocking(request.isolationLevel())) {
                return List.of();
            }
            List<LockHandle> handles = new ArrayList<>();
            try {
                // 3、逐行锁和 terminal gap 只消费剩余预算；授锁后重扫验证完整落点集合。
                acquireRangeLocks(position, request, mode, handles, deadline);
                BTreeCurrentReadRangePosition relocated = locateRange(index, range);
                if (sameRangePosition(position, relocated, request.isolationLevel())) {
                    return relocated.records().stream()
                            .map(hit -> hit.record().orElseThrow())
                            .toList();
                }
            } catch (RuntimeException e) {
                closeAll(handles);
                throw e;
            }
            // 4、重定位失败逆序释放本轮锁，再以同一 deadline 重试，不能延长 statement 等待上限。
            closeAll(handles);
        }
        throw new BTreeCurrentReadRelocationException(
                "current-read range relocation exceeded retries for index " + index.indexId());
    }

    private BTreeCurrentReadPosition locate(BTreeIndex index, SearchKey key, boolean includeDeleted) {
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            BTreeCurrentReadPosition position = btree.locatePointForCurrentRead(mtr, index, key, includeDeleted);
            mtrManager.commit(mtr);
            return position;
        } catch (RuntimeException e) {
            mtrManager.rollbackUncommitted(mtr);
            throw e;
        }
    }

    private BTreeCurrentReadRangePosition locateRange(BTreeIndex index, BTreeScanRange range) {
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            BTreeCurrentReadRangePosition position = btree.locateRangeForCurrentRead(mtr, index, range);
            mtrManager.commit(mtr);
            return position;
        } catch (RuntimeException e) {
            mtrManager.rollbackUncommitted(mtr);
            throw e;
        }
    }

    private static void validatePoint(BTreeIndex index, SearchKey key,
                                      BTreeCurrentReadRequest request, BTreeCurrentReadMode mode) {
        if (index == null || key == null || request == null || mode == null) {
            throw new DatabaseValidationException("current-read point args must not be null");
        }
    }

    private static void validateUnique(BTreeIndex index, SearchKey key, BTreeCurrentReadRequest request) {
        if (index == null || key == null || request == null) {
            throw new DatabaseValidationException("unique current-read args must not be null");
        }
        if (!index.physicalUnique()) {
            throw new DatabaseValidationException("unique current-read requires unique index: " + index.indexId());
        }
    }

    private static void validateRange(BTreeIndex index, BTreeScanRange range,
                                      BTreeCurrentReadRequest request, BTreeCurrentReadMode mode) {
        if (index == null || range == null || request == null || mode == null) {
            throw new DatabaseValidationException("current-read range args must not be null");
        }
    }

    /**
     * 按B+Tree 索引并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param position 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param handles 参与 {@code acquireRangeLocks} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param deadline 本次 range current-read 创建的绝对等待预算；全部锁申请只能消费其剩余值
     */
    private void acquireRangeLocks(BTreeCurrentReadRangePosition position, BTreeCurrentReadRequest request,
                                   BTreeCurrentReadMode mode, List<LockHandle> handles,
                                   WaitDeadline deadline) {
        for (BTreeCurrentReadPosition record : position.records()) {
            TransactionLockKey key = usesNextKeyLocking(request.isolationLevel())
                    ? record.nextKey().orElseThrow()
                    : record.recordKey().orElseThrow();
            TransactionLockMode lockMode = rangeRecordLockMode(request.isolationLevel(), mode);
            handles.add(lockManager.acquire(request.owner(), key, lockMode,
                    deadline.remaining("range record lock")));
        }
        if (usesNextKeyLocking(request.isolationLevel())
                && position.terminalGap().isPresent()) {
            handles.add(lockManager.acquire(request.owner(), position.terminalGap().orElseThrow(),
                    rangeGapLockMode(mode), deadline.remaining("range terminal gap lock")));
        }
    }

    /**
     * 单次 B+Tree current-read 的 monotonic 绝对等待预算。逐行授锁和 relocation 只能消费剩余值，
     * 从而让 request 的 timeout 保持“整次操作上限”语义，而不是“每个锁各自上限”。
     *
     * @param startedNanos 创建预算时的单调时钟读数；只用于同一 JVM 内计算相对耗时
     * @param budgetNanos 正等待预算；{@link Long#MAX_VALUE} 表示 {@link Duration#toNanos()} 溢出的超长预算
     */
    private record WaitDeadline(long startedNanos, long budgetNanos) {

        /**
         * 从已校验的正 Duration 建立一次绝对预算；转换溢出时按可表达的最大等待处理。
         *
         * @param timeout current-read 对全部锁等待和重定位共享的正上限
         * @return 绑定当前 monotonic 起点的不可变预算
         */
        private static WaitDeadline after(Duration timeout) {
            long nanos;
            try {
                nanos = timeout.toNanos();
            } catch (ArithmeticException overflow) {
                nanos = Long.MAX_VALUE;
            }
            return new WaitDeadline(System.nanoTime(), nanos);
        }

        /**
         * 计算当前阶段可交给 LockManager 的剩余正时长；预算耗尽时在创建新等待请求前失败，
         * 因而不会把已过期语句重新挂入 wait queue。
         *
         * @param stage 当前即将申请的锁阶段，仅用于超时诊断
         * @return 严格为正、且不超过原始总预算的剩余时长
         * @throws LockWaitTimeoutException 总预算已耗尽时抛出；调用方负责释放本轮已授予锁
         */
        private Duration remaining(String stage) {
            long elapsed = System.nanoTime() - startedNanos;
            long remaining = budgetNanos == Long.MAX_VALUE ? Long.MAX_VALUE : budgetNanos - elapsed;
            if (remaining <= 0) {
                throw new LockWaitTimeoutException("B+Tree current-read deadline exhausted at " + stage);
            }
            return Duration.ofNanos(remaining);
        }
    }

    private static TransactionLockMode pointLockMode(boolean hit, BTreeCurrentReadMode mode) {
        if (hit) {
            return mode == BTreeCurrentReadMode.FOR_SHARE
                    ? TransactionLockMode.REC_S
                    : TransactionLockMode.REC_X;
        }
        return mode == BTreeCurrentReadMode.FOR_SHARE
                ? TransactionLockMode.GAP_S
                : TransactionLockMode.GAP_X;
    }

    private static TransactionLockMode rangeRecordLockMode(IsolationLevel isolationLevel,
                                                           BTreeCurrentReadMode mode) {
        if (usesNextKeyLocking(isolationLevel)) {
            return mode == BTreeCurrentReadMode.FOR_SHARE
                    ? TransactionLockMode.NEXT_KEY_S
                    : TransactionLockMode.NEXT_KEY_X;
        }
        return mode == BTreeCurrentReadMode.FOR_SHARE
                ? TransactionLockMode.REC_S
                : TransactionLockMode.REC_X;
    }

    private static TransactionLockMode rangeGapLockMode(BTreeCurrentReadMode mode) {
        return mode == BTreeCurrentReadMode.FOR_SHARE
                ? TransactionLockMode.GAP_S
                : TransactionLockMode.GAP_X;
    }

    private static boolean samePointPosition(BTreeCurrentReadPosition before,
                                             BTreeCurrentReadPosition after) {
        if (before.hit() != after.hit()) {
            return false;
        }
        if (before.hit()) {
            return before.recordKey().orElseThrow().equals(after.recordKey().orElseThrow());
        }
        return before.gapKey().equals(after.gapKey());
    }

    private static boolean sameRangePosition(BTreeCurrentReadRangePosition before,
                                             BTreeCurrentReadRangePosition after,
                                             IsolationLevel isolationLevel) {
        if (before.records().size() != after.records().size()) {
            return false;
        }
        for (int i = 0; i < before.records().size(); i++) {
            BTreeCurrentReadPosition left = before.records().get(i);
            BTreeCurrentReadPosition right = after.records().get(i);
            if (usesNextKeyLocking(isolationLevel)) {
                if (!left.nextKey().orElseThrow().equals(right.nextKey().orElseThrow())) {
                    return false;
                }
            } else if (!left.recordKey().orElseThrow().equals(right.recordKey().orElseThrow())) {
                return false;
            }
        }
        return !usesNextKeyLocking(isolationLevel)
                || before.terminalGap().equals(after.terminalGap());
    }

    /**
     * RU 的锁定写/显式 locking read 与 RC 一样不为 miss 取得 gap 锁；RU 的“脏读”只影响普通读版本选择，
     * 不允许意外扩大为 RR next-key 语义。
     */
    private static boolean usesReadCommittedLocking(IsolationLevel isolationLevel) {
        return isolationLevel == IsolationLevel.READ_UNCOMMITTED
                || isolationLevel == IsolationLevel.READ_COMMITTED;
    }

    /**
     * SERIALIZABLE locking read 必须与 RR 一样覆盖记录前 gap 和 terminal gap，防止显式事务中的普通
     * SELECT（已由 Session 提升为 FOR SHARE）出现幻读。
     */
    private static boolean usesNextKeyLocking(IsolationLevel isolationLevel) {
        return isolationLevel == IsolationLevel.REPEATABLE_READ
                || isolationLevel == IsolationLevel.SERIALIZABLE;
    }

    /**
     * 释放本方法拥有的B+Tree 索引资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param handles 参与 {@code closeAll} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    private static void closeAll(List<LockHandle> handles) {
        for (int i = handles.size() - 1; i >= 0; i--) {
            handles.get(i).close();
        }
    }
}
