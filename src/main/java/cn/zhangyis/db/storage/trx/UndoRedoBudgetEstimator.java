package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;

import java.util.Collection;
import java.util.List;

/** Undo append/finalization 的领域 workload 估算器；只消费事务/segment 只读事实，不访问 redo capacity。 */
public final class UndoRedoBudgetEstimator {

    private UndoRedoBudgetEstimator() {
    }

    /** fresh 首写覆盖 FSP create，cached 首写覆盖 page3 owner move/header reset，existing 只覆盖 append/grow。
     *
     * @param acquisition 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code append} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoBudgetWorkload append(UndoSegmentAcquisition acquisition) {
        if (acquisition == null) {
            throw new DatabaseValidationException("undo append acquisition must not be null");
        }
        return RedoBudgetWorkload.pageImages(switch (acquisition) {
            case ALLOCATE_NEW -> 12L;
            case REUSE_CACHED -> 8L;
            case REUSE_FREE -> 10L;
            case APPEND_EXISTING -> 4L;
        });
    }

    /**
     * 兼容只区分首写与追加的预算调用方。生产写路径必须传入明确的 segment 获取方式，
     * 否则无法表达 cached segment 激活所需的 page 3 owner 转移与首页重置开销。
     *
     * @param firstUndoWrite 当前对象是否处于首次创建、首次写入或复用分支；该事实决定初始化、redo 和失败补偿路径
     * @return {@code append} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public static RedoBudgetWorkload append(boolean firstUndoWrite) {
        return append(firstUndoWrite ? UndoSegmentAcquisition.ALLOCATE_NEW
                : UndoSegmentAcquisition.APPEND_EXISTING);
    }

    /** external payload 每页覆盖 FSP allocation、PAGE_INIT 与完整 PAGE_BYTES，按 LOB 同级每页追加 8 份余量。
     *
     * @param acquisition 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param externalPages 参与 {@code append} 的上界或规格值 {@code externalPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return {@code append} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoBudgetWorkload append(UndoSegmentAcquisition acquisition, int externalPages) {
        if (acquisition == null) {
            throw new DatabaseValidationException("undo append acquisition must not be null");
        }
        if (externalPages < 0) {
            throw new DatabaseValidationException("external undo page count must not be negative: " + externalPages);
        }
        try {
            long base = switch (acquisition) {
                case ALLOCATE_NEW -> 12L;
                case REUSE_CACHED -> 8L;
                case REUSE_FREE -> 10L;
                case APPEND_EXISTING -> 4L;
            };
            return RedoBudgetWorkload.pageImages(Math.addExact(base,
                    Math.multiplyExact(8L, externalPages)));
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("external undo redo workload overflows", error);
        }
    }

    /**
     * drop 固定覆盖 page0/page2/page3、inode/slot 与发布边界；每 fragment 计两份、每 extent 计四份元数据余量。
     *
     * @param plan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param includesTerminalDelta 当前算法是否纳入终态增量、同步压力、磁盘来源、根节点或周期上界校验；用于选择对应的不变量检查分支
     * @return {@code finalization} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoBudgetWorkload finalization(UndoSegmentDropPlan plan, boolean includesTerminalDelta) {
        if (plan == null) {
            throw new DatabaseValidationException("undo finalization drop plan must not be null");
        }
        try {
            long pages = 7L;
            pages = Math.addExact(pages, Math.multiplyExact(2L, plan.fragmentPageCount()));
            pages = Math.addExact(pages, Math.multiplyExact(4L, plan.extentCount()));
            if (includesTerminalDelta) {
                pages = Math.addExact(pages, 1L);
            }
            return RedoBudgetWorkload.pageImages(pages);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("undo finalization redo workload overflows", error);
        }
    }

    /** 多 segment 原子终结只计算一次 batch/page3/terminal 固定开销，各 drop plan 的动态规模分别累加。
     *
     * @param plans 参与 {@code finalization} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param includesTerminalDelta 当前算法是否纳入终态增量、同步压力、磁盘来源、根节点或周期上界校验；用于选择对应的不变量检查分支
     * @return {@code finalization} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoBudgetWorkload finalization(Collection<UndoSegmentDropPlan> plans,
                                                   boolean includesTerminalDelta) {
        if (plans == null || plans.isEmpty() || plans.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("undo finalization plans must not be empty or contain null");
        }
        try {
            long pages = 7L;
            for (UndoSegmentDropPlan plan : plans) {
                pages = Math.addExact(pages, Math.multiplyExact(2L, plan.fragmentPageCount()));
                pages = Math.addExact(pages, Math.multiplyExact(4L, plan.extentCount()));
                pages = Math.addExact(pages, 2L);
            }
            if (includesTerminalDelta) {
                pages = Math.addExact(pages, 1L);
            }
            return RedoBudgetWorkload.pageImages(pages);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("multi-segment undo finalization workload overflows", error);
        }
    }

    /**
     * cache/free/drop 混合终结预算。drop 继续按 fragment/extent 规模计费；每个 reusable segment 额外覆盖 page3 owner
     * transition、首页 header reset 与重复 metadata delta 余量。允许 droppedPlans 为空，但 cachedCount 必须为正。
     *
     * @param droppedPlans 参与 {@code finalization} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param cachedCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param includesTerminalDelta 当前算法是否纳入终态增量、同步压力、磁盘来源、根节点或周期上界校验；用于选择对应的不变量检查分支
     * @return {@code finalization} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public static RedoBudgetWorkload finalization(Collection<UndoSegmentDropPlan> droppedPlans,
                                                   int cachedCount,
                                                   boolean includesTerminalDelta) {
        return finalization(droppedPlans, cachedCount, 0, includesTerminalDelta);
    }

    /** cache/free/drop 混合终结预算；FREE 每段覆盖 page3 base、首页 reset 与相邻节点 relink。
     *
     * @param droppedPlans 参与 {@code finalization} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param cachedCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param freeCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param includesTerminalDelta 当前算法是否纳入终态增量、同步压力、磁盘来源、根节点或周期上界校验；用于选择对应的不变量检查分支
     * @return {@code finalization} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoBudgetWorkload finalization(Collection<UndoSegmentDropPlan> droppedPlans,
                                                   int cachedCount, int freeCount,
                                                   boolean includesTerminalDelta) {
        if (droppedPlans == null || droppedPlans.stream().anyMatch(java.util.Objects::isNull)
                || cachedCount < 0 || freeCount < 0
                || droppedPlans.isEmpty() && cachedCount == 0 && freeCount == 0) {
            throw new DatabaseValidationException("mixed undo finalization workload is invalid");
        }
        try {
            long pages = 7L;
            for (UndoSegmentDropPlan plan : droppedPlans) {
                pages = Math.addExact(pages, Math.multiplyExact(2L, plan.fragmentPageCount()));
                pages = Math.addExact(pages, Math.multiplyExact(4L, plan.extentCount()));
                pages = Math.addExact(pages, 2L);
            }
            pages = Math.addExact(pages, Math.multiplyExact(4L, cachedCount));
            pages = Math.addExact(pages, Math.multiplyExact(6L, freeCount));
            if (includesTerminalDelta) {
                pages = Math.addExact(pages, 1L);
            }
            return RedoBudgetWorkload.pageImages(pages);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("mixed undo finalization workload overflows", error);
        }
    }

    /** UPDATE header 加 terminal delta；mixed commit 再合并 INSERT drop plan。
     *
     * @param insertDropPlan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code commit} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public static RedoBudgetWorkload commit(UndoSegmentDropPlan insertDropPlan) {
        if (insertDropPlan == null) {
            return RedoBudgetWorkload.pageImages(3L);
        }
        return finalization(java.util.List.of(insertDropPlan), true).plus(RedoBudgetWorkload.pageImages(2L));
    }

    /** mixed/INSERT commit 根据最终 disposition 选择 drop 或 cached header reset 上界。
     *
     * @param insertPlan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param cacheInsert 既有物理或缓存步骤是否已经发生；该事实用于避免重复修改、重复补偿或错误发布中间状态
     * @return {@code commit} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public static RedoBudgetWorkload commit(UndoSegmentDropPlan insertPlan, boolean cacheInsert) {
        return commit(insertPlan, cacheInsert, false);
    }

    /** mixed/INSERT commit 根据最终 cache/free/drop disposition 选择完整终结预算。
     *
     * @param insertPlan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param cacheInsert 既有物理或缓存步骤是否已经发生；该事实用于避免重复修改、重复补偿或错误发布中间状态
     * @param freeInsert 既有物理或缓存步骤是否已经发生；该事实用于避免重复修改、重复补偿或错误发布中间状态
     * @return {@code commit} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoBudgetWorkload commit(UndoSegmentDropPlan insertPlan,
                                            boolean cacheInsert, boolean freeInsert) {
        if (insertPlan == null) {
            return commit(null);
        }
        if (cacheInsert && freeInsert) {
            throw new DatabaseValidationException("one undo segment cannot be both cached and free");
        }
        return finalization(cacheInsert || freeInsert ? List.of() : List.of(insertPlan),
                cacheInsert ? 1 : 0, freeInsert ? 1 : 0, true)
                .plus(RedoBudgetWorkload.pageImages(2L));
    }

    /**
     * XA phase one 只修改一或两个 first-page state并追加一个逻辑事务 delta；每页保留一份完整 image 余量。
     *
     * @param undoLogCount 当前事务普通 INSERT/UPDATE undo log 数量，v1 只允许 1..2
     * @return 覆盖 first-page metadata与 transaction-state logical redo 的保守工作量
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoBudgetWorkload prepare(int undoLogCount) {
        if (undoLogCount < 1 || undoLogCount > 2) {
            throw new DatabaseValidationException(
                    "XA prepare ordinary undo log count must be between 1 and 2: " + undoLogCount);
        }
        return RedoBudgetWorkload.pageImages(undoLogCount + 2L);
    }
}
