package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLoadInterceptor;
import cn.zhangyis.db.storage.buf.PendingPagePublication;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.RedoAppendBudget;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.page.PageType;

import java.time.Duration;
import java.util.List;

/**
 * Buffer Pool 发布前 Change Buffer merge 编排器。它先在独立只读 MTR 检查 bitmap/扫描全局树，再在一个独立写 MTR
 * adopt LOADING target、局部应用、consume 全局记录并清 bitmap；写 MTR 提交完成之前 LOADING future 不会唤醒。
 */
public final class ChangeBufferPageMergeInterceptor implements PageLoadInterceptor {

    /** 单页 mutation 硬上限；buffer 路径必须在达到上限前回退直写，防止一次 loader 无界占用内存/redo。 */
    public static final int MAX_MUTATIONS_PER_PAGE = SecondaryIndexMutationCoordinator.MAX_PENDING_PER_PAGE;
    /** 系统/用户 bitmap 仓储。 */
    private final ChangeBufferBitmapRepository bitmapRepository;
    /** 全局 mutation B+Tree store。 */
    private final ChangeBufferStore store;
    /** 目标 leaf 单页幂等执行器。 */
    private final ChangeBufferPageMerger pageMerger;
    /** 提供独立 scan/merge MTR 与 redo admission 的组合根 manager。 */
    private final MiniTransactionManager mtrManager;
    /** 同目标页 buffer/merge/drain 串行 gate。 */
    private final ChangeBufferPageGate pageGate;
    /** gate 有界等待上限。 */
    private final Duration gateTimeout;
    /** bitmap 固定页过滤使用的实例页大小。 */
    private final PageSize pageSize;
    /** 与 append/DDL/worker 共享的提交后计数 owner。 */
    private final ChangeBufferCounters counters;

    /**
     * @param bitmapRepository 与用户 tablespace 共用的 bitmap 仓储
     * @param store system.ibd 全局树 store
     * @param pageMerger exact-version 单页 merger
     * @param mtrManager 引擎共享 MTR manager
     * @param pageGate 前台 buffer/loader/background 共用 gate
     * @param gateTimeout 正等待上限
     * @param pageSize 实例页大小
     */
    public ChangeBufferPageMergeInterceptor(ChangeBufferBitmapRepository bitmapRepository,
                                            ChangeBufferStore store,
                                            ChangeBufferPageMerger pageMerger,
                                            MiniTransactionManager mtrManager,
                                            ChangeBufferPageGate pageGate,
                                            Duration gateTimeout,
                                            PageSize pageSize) {
        this(bitmapRepository, store, pageMerger, mtrManager, pageGate, gateTimeout,
                pageSize, new ChangeBufferCounters());
    }

    /**
     * 创建使用组合根共享统计 owner 的发布前拦截器。
     *
     * @param bitmapRepository 用户 bitmap 仓储
     * @param store 全局 mutation store
     * @param pageMerger 已 adopt leaf 的局部执行器
     * @param mtrManager detached scan/merge MTR 工厂
     * @param pageGate 同目标页 append/merge/drain gate
     * @param gateTimeout 有界等待上限
     * @param pageSize 实例固定页大小
     * @param counters 引擎共享提交后计数 owner
     */
    public ChangeBufferPageMergeInterceptor(ChangeBufferBitmapRepository bitmapRepository,
                                            ChangeBufferStore store,
                                            ChangeBufferPageMerger pageMerger,
                                            MiniTransactionManager mtrManager,
                                            ChangeBufferPageGate pageGate,
                                            Duration gateTimeout,
                                            PageSize pageSize,
                                            ChangeBufferCounters counters) {
        if (bitmapRepository == null || store == null || pageMerger == null || mtrManager == null
                || pageGate == null || gateTimeout == null || pageSize == null
                || counters == null || gateTimeout.isZero() || gateTimeout.isNegative()) {
            throw new DatabaseValidationException("change buffer merge interceptor dependencies are invalid");
        }
        this.bitmapRepository = bitmapRepository;
        this.store = store;
        this.pageMerger = pageMerger;
        this.mtrManager = mtrManager;
        this.pageGate = pageGate;
        this.gateTimeout = gateTimeout;
        this.pageSize = pageSize;
        this.counters = counters;
    }

    /**
     * 在目标页发布前完成全部 pending mutation；system.ibd 和 bitmap 固定页直接跳过，防止递归拦截。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>过滤 SpaceId 0 与重复 bitmap 页，再以 timeout 取得 per-target gate。</li>
     *     <li>独立只读 MTR 读取 bitmap；无 buffered 位或 internal 页立即返回，不打开全局树。</li>
     *     <li>另一独立只读 MTR 扫描该页全部 mutation；空记录修复 stale bitmap，超单页上限 fail-stop。</li>
     *     <li>独立写 MTR adopt target并应用批次，逐条 consume 后写回 free class/清 buffered；commit 后才允许 Buffer Pool 发布。</li>
     * </ol>
     *
     * @param publication Buffer Pool 唯一 LOADING owner 提供的发布前能力
     */
    @Override
    public void beforePublish(PendingPagePublication publication) {
        try {
            mergeBeforePublish(publication);
        } catch (RuntimeException failure) {
            counters.recordMergeFailure();
            throw failure;
        }
    }

    /**
     * 执行实际发布前过滤与 merge；异常由公开入口统一计入失败统计后原样传播。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 publication 并过滤系统、bitmap 和非 INDEX 页，避免递归或误读普通数据页。</li>
     *     <li>取得目标 gate 后以 detached 只读 MTR 检查 bitmap，未标 pending 时零写返回。</li>
     *     <li>释放 bitmap MTR 后以第二个 detached MTR 扫描完整批次，空批次只修复 stale bit。</li>
     *     <li>写 MTR adopt 尚未发布的 target，局部 apply、global consume 与 bitmap 更新共享一个 redo batch。</li>
     * </ol>
     *
     * @param publication Buffer Pool LOADING 唯一 owner 授予的一次性页面发布能力
     * @throws ChangeBufferMergeCapacityException 单页批次超限或局部 INSERT 容量不足时抛出，页面不得发布
     * @throws ChangeBufferFormatException bitmap、global record、metadata 或目标页身份损坏时抛出，证据保持不消费
     */
    private void mergeBeforePublish(PendingPagePublication publication) {
        // 1、系统 Change Buffer 自身和 bitmap page 永不再次走 merge 查询，切断递归加载链。
        if (publication == null) {
            throw new DatabaseValidationException("pending page publication must not be null");
        }
        PageId target = publication.pageId();
        if (target.spaceId().equals(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)
                || ChangeBufferBitmapLayout.isBitmapPage(pageSize, target.pageNo())
                || !bitmapRepository.supportsTarget(target)
                || publication.loadedPageType() != PageType.INDEX) {
            return;
        }
        try (ChangeBufferPageGate.Lease ignored = pageGate.acquire(target, gateTimeout)) {
            // 2、bitmap 检查使用短只读子 MTR；目标页尚未 adopt，不与它共享 fix/latch。
            ChangeBufferBitmapState bitmap = mtrManager.executeDetached(RedoAppendBudget.readOnly(),
                    read -> bitmapRepository.read(read, target));
            if (!bitmap.buffered() || bitmap.changeBufferInternal()) {
                return;
            }
            // 3、scan 子 MTR在 merge 写 MTR 前完整释放 global tree S latch，避免后续同页 S→X 升级。
            List<ChangeBufferMutation> mutations = mtrManager.executeDetached(RedoAppendBudget.readOnly(),
                    read -> store.scanPage(read, target, MAX_MUTATIONS_PER_PAGE + 1));
            if (mutations.isEmpty()) {
                repairStaleBitmap(target, bitmap);
                return;
            }
            if (mutations.size() > MAX_MUTATIONS_PER_PAGE) {
                throw new ChangeBufferMergeCapacityException(
                        "change buffer target exceeds per-page mutation limit: " + target);
            }
            // 4、一次 redo batch 原子覆盖 target apply、global consume、header count 和 bitmap clear。
            mtrManager.executeDetached(mtrManager.budgetFor(RedoBudgetPurpose.CHANGE_BUFFER_MERGE), merge -> {
                try (var order = merge.allowOutOfOrderPageLatch(
                        "change-buffer prepublish merge: pending target is unreachable to ordinary fixers; "
                                + "global ibuf/bitmap paths never wait for a user target latch")) {
                    PageGuard targetGuard = merge.adoptPendingPage(publication);
                    ChangeBufferMergeResult result = pageMerger.apply(targetGuard, mutations);
                    for (ChangeBufferMutation mutation : mutations) {
                        if (!store.consume(merge, mutation)) {
                            throw new ChangeBufferStateException(
                                    "scanned change buffer mutation disappeared before consume: "
                                            + target + "/" + mutation.sequence());
                        }
                    }
                    bitmapRepository.write(merge, target, new ChangeBufferBitmapState(
                            result.freeSpaceClass(), false, bitmap.changeBufferInternal()));
                    return result;
                }
            });
            counters.recordMerged(mutations.size());
        }
    }

    private void repairStaleBitmap(PageId target, ChangeBufferBitmapState oldState) {
        mtrManager.executeDetached(mtrManager.budgetFor(RedoBudgetPurpose.CHANGE_BUFFER_MERGE), write -> {
            bitmapRepository.write(write, target, new ChangeBufferBitmapState(
                    oldState.freeSpaceClass(), false, oldState.changeBufferInternal()));
            return null;
        });
    }
}
