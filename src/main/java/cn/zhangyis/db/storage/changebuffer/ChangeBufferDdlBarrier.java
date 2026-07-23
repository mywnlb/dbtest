package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Comparator;
import java.util.function.Predicate;

/**
 * DDL 回收索引或表空间前的 Change Buffer 屏障。上层必须已经用 MDL/DDL 状态阻断新 DML；本类再按目标页 gate
 * 原子 discard 对应全局记录并修正 bitmap，成功返回后物理 segment/file 才可回收。
 *
 * <p>DROP 不需要把即将不可达的 entry 合并进用户页，因此使用 discard 而不是产生无意义目标页 IO；全局记录删除、
 * header pending 扣减与 bitmap buffered 位更新仍在同一 MTR，崩溃重放后不会留下悬空 identity。</p>
 */
@Slf4j
public final class ChangeBufferDdlBarrier {

    /** system.ibd 全局 mutation 树入口；discard 只通过该接口消费持久证据。 */
    private final ChangeBufferStore store;
    /** 用户表空间 bitmap 仓储；由目标页 gate 与同一 MTR 保护 buffered 位变更。 */
    private final ChangeBufferBitmapRepository bitmaps;
    /** append、merge 与 DDL drain 共用的目标页串行化边界。 */
    private final ChangeBufferPageGate pageGate;
    /** 创建 scan/discard/reset 所需短 MTR，并负责 WAL 提交或失败释放。 */
    private final MiniTransactionManager mtrManager;
    /** DDL 完成后移除旧 exact-version 二级索引绑定的进程内目录。 */
    private final ChangeBufferMetadataCatalog metadataCatalog;
    /** 与 append/merge/worker 共享的进程级提交后计数 owner。 */
    private final ChangeBufferCounters counters;

    /**
     * @param store 全局 mutation store
     * @param bitmaps 用户表空间 bitmap 仓储
     * @param pageGate 与 append/merge 共用的 per-target gate
     * @param mtrManager 同一 redo 域的短事务工厂
     * @param metadataCatalog DDL 完成后移除旧 exact-version 缓存的目录
     */
    public ChangeBufferDdlBarrier(ChangeBufferStore store, ChangeBufferBitmapRepository bitmaps,
                                  ChangeBufferPageGate pageGate, MiniTransactionManager mtrManager,
                                  ChangeBufferMetadataCatalog metadataCatalog) {
        this(store, bitmaps, pageGate, mtrManager, metadataCatalog, new ChangeBufferCounters());
    }

    /**
     * 创建使用组合根共享统计 owner 的 DDL 屏障。
     *
     * @param store 全局 mutation store
     * @param bitmaps 用户 bitmap 仓储
     * @param pageGate append/merge/drain 共用 gate
     * @param mtrManager scan/discard MTR 工厂
     * @param metadataCatalog exact-version 目录
     * @param counters 引擎共享提交后统计 owner
     */
    public ChangeBufferDdlBarrier(ChangeBufferStore store, ChangeBufferBitmapRepository bitmaps,
                                  ChangeBufferPageGate pageGate, MiniTransactionManager mtrManager,
                                  ChangeBufferMetadataCatalog metadataCatalog,
                                  ChangeBufferCounters counters) {
        if (store == null || bitmaps == null || pageGate == null
                || mtrManager == null || metadataCatalog == null || counters == null) {
            throw new DatabaseValidationException("change buffer DDL barrier dependencies must not be null");
        }
        this.store = store;
        this.bitmaps = bitmaps;
        this.pageGate = pageGate;
        this.mtrManager = mtrManager;
        this.metadataCatalog = metadataCatalog;
        this.counters = counters;
    }

    /**
     * 丢弃一个已被 DD 标记不可见的二级索引全部 mutation。
     *
     * @param tableId 已持有排他 DDL 所有权的正表 id
     * @param indexId 即将回收的正二级索引 id
     * @param timeout 整个 barrier 的正等待上限
     * @return 实际从全局树消费的记录数
     */
    public long discardIndex(long tableId, long indexId, Duration timeout) {
        if (tableId <= 0 || indexId <= 0) {
            throw new DatabaseValidationException("change buffer DDL index identities must be positive");
        }
        long discarded = discard(mutation -> mutation.tableId() == tableId
                && mutation.indexId() == indexId, timeout, true);
        metadataCatalog.unregisterIndex(tableId, indexId);
        if (discarded > 0) {
            log.info("discarded Change Buffer records before index retirement: table={} index={} count={}",
                    tableId, indexId, discarded);
        }
        return discarded;
    }

    /**
     * 丢弃即将 DROP/TRUNCATE/DISCARD 的整个目标表空间 mutation。
     *
     * @param spaceId 在生命周期独占 lease 前仍可普通访问的用户空间；SpaceId 0 不允许丢弃
     * @param timeout 整个 barrier 的正等待上限
     * @return 实际消费记录数
     */
    public long discardSpace(SpaceId spaceId, Duration timeout) {
        if (spaceId == null || spaceId.equals(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)) {
            throw new DatabaseValidationException("change buffer DDL space must be a non-system tablespace");
        }
        long discarded = discard(
                mutation -> mutation.targetPageId().spaceId().equals(spaceId), timeout, true);
        if (discarded > 0) {
            log.info("discarded Change Buffer records before tablespace retirement: space={} count={}",
                    spaceId.value(), discarded);
        }
        return discarded;
    }

    /**
     * 隔离/损坏空间的文件已不能安全读取 bitmap 时，只消费 system.ibd 中的悬空 mutation 证据。
     * 调用方必须保证该物理 incarnation 永不重新挂载；因此不更新即将删除/隔离文件内的 bitmap 是安全的。
     *
     * @param spaceId 已被 DD 标记 recovery-unavailable 的非系统空间
     * @param timeout 全局记录 discard 的正等待上限
     * @return 实际消费记录数
     */
    public long discardUnavailableSpace(SpaceId spaceId, Duration timeout) {
        if (spaceId == null || spaceId.equals(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)) {
            throw new DatabaseValidationException("change buffer unavailable space must be non-system");
        }
        long discarded = discard(
                mutation -> mutation.targetPageId().spaceId().equals(spaceId), timeout, false);
        if (discarded > 0) {
            log.info("discarded Change Buffer records for unavailable tablespace: space={} count={}",
                    spaceId.value(), discarded);
        }
        return discarded;
    }

    /**
     * 重建刚挂载但尚未恢复 NORMAL 的 IMPORT 文件全部重复 bitmap 页。调用方必须已在本方法之前丢弃旧
     * incarnation 的全局 mutation，并持有目标表空间独占 lifecycle lease；本方法不访问 system.ibd，避免形成
     * “用户空间 X lease → 系统空间 lease”的反向依赖。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验用户 SpaceId、物理文件页数和统一 deadline，确保 page1 至少存在。</li>
     *     <li>按 bitmap 公式从 page1 起稳定枚举当前文件覆盖区间，每轮先检查剩余等待预算。</li>
     *     <li>每个 bitmap 页使用独立 CHANGE_BUFFER_MERGE 预算写 MTR 清空 body，限制单批 redo 和 latch 集合。</li>
     *     <li>逐页提交已完成前缀；中途失败时 page0 仍为 DISCARDED，DDL recovery 可幂等重试未完成页。</li>
     * </ol>
     *
     * @param spaceId 已复制并以 recovery 模式挂载、尚未恢复 NORMAL 的用户表空间
     * @param currentSizeInPages PageStore 对导入文件观察到的稳定物理页数
     * @param timeout 整个 bitmap 重建阶段的正等待上限
     * @throws DatabaseValidationException 参数为空、系统空间、文件过小或 timeout 非正时抛出
     * @throws ChangeBufferFormatException 公式指向的页面不是有效 IBUF_BITMAP 时抛出，文件不得开放流量
     */
    public void resetImportedSpaceBitmaps(
            SpaceId spaceId, PageNo currentSizeInPages, Duration timeout) {
        // 1、IMPORT 文件必须至少包含 page0/page1；旧 incarnation 的全局记录由调用方在用户 X lease 外清理。
        requireTimeout(timeout);
        if (spaceId == null || spaceId.equals(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)
                || currentSizeInPages == null || currentSizeInPages.value() <= 1L) {
            throw new DatabaseValidationException(
                    "change buffer imported bitmap reset requires a non-system space with page1");
        }
        long deadline = deadline(timeout);
        long interval = bitmaps.bitmapPageInterval();
        long resetPages = 0L;

        // 2、枚举公式覆盖的全部 bitmap identity；大文件若未预留重复页会在类型复核时明确失败。
        for (long pageNo = 1L; pageNo < currentSizeInPages.value();
             pageNo = Math.addExact(pageNo, interval)) {
            remaining(deadline);
            PageId bitmapPage = PageId.of(spaceId, PageNo.of(pageNo));

            // 3、单页单 MTR 避免 IMPORT 大文件形成无界 redo batch 或同时持有大量 bitmap X latch。
            MiniTransaction reset = mtrManager.begin(
                    mtrManager.budgetFor(RedoBudgetPurpose.CHANGE_BUFFER_MERGE));
            try {
                bitmaps.resetBitmapPage(reset, bitmapPage);
                // 4、已提交前缀只包含“更保守的全零”状态；失败重试重复清零保持幂等。
                mtrManager.commit(reset);
                resetPages++;
            } catch (RuntimeException failure) {
                rollbackActive(reset, failure);
                throw failure;
            }
        }
        log.info("rebuilt imported Change Buffer bitmap pages: space={} pages={}",
                spaceId.value(), resetPages);
    }

    /**
     * <p>数据流：</p>
     * <ol>
     *     <li>读取一次全局快照并提取匹配目标页；只读 MTR 结束后才等待任何 page gate。</li>
     *     <li>按稳定 PageId 顺序逐页有界取得 gate，并重新扫描该页，防止后台 merge 已经消费快照记录。</li>
     *     <li>同一写 MTR消费全部匹配记录，再按未匹配余量更新 buffered 位；不打开即将丢弃的目标数据页。</li>
     *     <li>每页提交后累计实际 consume 数；任一失败停止 DDL，已提交前缀可在重试时幂等跳过。</li>
     * </ol>
     *
     * @param predicate 选择本次退休 index/space identity 的纯内存谓词
     * @param timeout 整个多 target barrier 的正等待上限
     * @param updateBitmap 目标文件仍可读时为 true；不可恢复且保证永不重挂载的空间为 false
     * @return 已从全局树实际消费并提交的 mutation 数
     * @throws ChangeBufferPageGateTimeoutException deadline 耗尽时抛出，物理 DDL 不得继续
     * @throws ChangeBufferFormatException header/tree/bitmap 证据矛盾时抛出，已提交前缀仍可由恢复识别
     */
    private long discard(Predicate<ChangeBufferMutation> predicate, Duration timeout,
                         boolean updateBitmap) {
        // 1、物化全局一致快照并提取目标集合；此时不持任何 gate 或用户页资源。
        requireTimeout(timeout);
        long deadline = deadline(timeout);
        List<ChangeBufferMutation> snapshot = scanAll();
        Set<PageId> targets = new LinkedHashSet<>();
        snapshot.stream().filter(predicate).map(ChangeBufferMutation::targetPageId).forEach(targets::add);
        long consumed = 0L;
        Comparator<PageId> physicalOrder = Comparator
                .comparingInt((PageId page) -> page.spaceId().value())
                .thenComparingLong(page -> page.pageNo().value());
        // 2、稳定物理顺序逐 target 等 gate，避免多个 DDL owner 形成条带锁顺序环。
        for (PageId target : targets.stream().sorted(physicalOrder).toList()) {
            Duration remaining = remaining(deadline);
            try (ChangeBufferPageGate.Lease ignored = pageGate.acquire(target, remaining)) {
                List<ChangeBufferMutation> current = scanPage(target);
                List<ChangeBufferMutation> selected = current.stream().filter(predicate).toList();
                if (selected.isEmpty()) {
                    continue;
                }
                boolean pendingAfter = current.stream().anyMatch(predicate.negate());
                // 3、当前页全部 selected record、header count 与可读 bitmap 在同一 bounded 写 MTR 中改变。
                MiniTransaction write = mtrManager.begin(
                        mtrManager.budgetFor(RedoBudgetPurpose.CHANGE_BUFFER_MERGE));
                try {
                    long pageConsumed = 0L;
                    for (ChangeBufferMutation mutation : selected) {
                        if (store.consume(write, mutation)) {
                            pageConsumed++;
                        }
                    }
                    if (updateBitmap) {
                        ChangeBufferBitmapState state = bitmaps.readForUpdate(write, target);
                        bitmaps.write(write, target, new ChangeBufferBitmapState(
                                state.freeSpaceClass(), pendingAfter, state.changeBufferInternal()));
                    }
                    // 4、提交后才累计统计；失败前缀不伪装成已 discard，已提交旧前缀可幂等跳过。
                    mtrManager.commit(write);
                    consumed = Math.addExact(consumed, pageConsumed);
                    if (pageConsumed > 0) {
                        counters.recordDiscarded(pageConsumed);
                    }
                } catch (RuntimeException failure) {
                    rollbackActive(write, failure);
                    throw failure;
                }
            }
        }
        return consumed;
    }

    private List<ChangeBufferMutation> scanAll() {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            long pending = store.pendingOperations(read);
            if (pending >= Integer.MAX_VALUE) {
                throw new ChangeBufferStateException(
                        "DDL barrier pending record count exceeds materialization limit: " + pending);
            }
            int limit = Math.toIntExact(pending + 1L);
            List<ChangeBufferMutation> result = store.scanAll(read, limit);
            if (pending != result.size()) {
                throw new ChangeBufferFormatException(
                        "DDL barrier observed change buffer header/tree mismatch: header="
                                + pending + ", tree=" + result.size());
            }
            mtrManager.commit(read);
            return result;
        } catch (RuntimeException failure) {
            rollbackActive(read, failure);
            throw failure;
        }
    }

    private List<ChangeBufferMutation> scanPage(PageId target) {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            List<ChangeBufferMutation> result = store.scanPage(
                    read, target, SecondaryIndexMutationCoordinator.MAX_PENDING_PER_PAGE + 1);
            if (result.size() > SecondaryIndexMutationCoordinator.MAX_PENDING_PER_PAGE) {
                throw new ChangeBufferStateException("DDL barrier target exceeds mutation limit: " + target);
            }
            mtrManager.commit(read);
            return result;
        } catch (RuntimeException failure) {
            rollbackActive(read, failure);
            throw failure;
        }
    }

    private void rollbackActive(MiniTransaction mtr, RuntimeException primary) {
        if (mtr.state() == MiniTransactionState.ACTIVE) {
            try {
                mtrManager.rollbackUncommitted(mtr);
            } catch (RuntimeException releaseFailure) {
                primary.addSuppressed(releaseFailure);
            }
        }
    }

    private static void requireTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("change buffer DDL timeout must be positive");
        }
    }

    private static long deadline(Duration timeout) {
        try {
            return Math.addExact(System.nanoTime(), timeout.toNanos());
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static Duration remaining(long deadline) {
        long nanos = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new ChangeBufferPageGateTimeoutException("change buffer DDL barrier timed out");
        }
        return Duration.ofNanos(nanos);
    }
}
