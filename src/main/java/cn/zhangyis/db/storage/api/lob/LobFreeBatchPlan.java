package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;

import java.util.List;

/**
 * 一个 authoritative LOB segment 下的不可变批量释放计划。实例只能由 {@link LobStorage} 创建；执行时仍会
 * 重算 identity、page count 和 workload，防止陈旧或伪造计划越过恢复边界。
 */
public final class LobFreeBatchPlan {
    /** exact DD table binding 提供的释放授权；reference 自带的 segment identity 只能用于交叉校验。 */
    private final SegmentRef segment;
    /** 按 external first-page/column ordinal 稳定排序的 ownership；构造后不可变。 */
    private final List<LobFreeTarget> targets;
    /** 在写 MTR begin 前冻结的动态 redo 上界；覆盖整批 FSP free 与 PAGE_INIT。 */
    private final RedoBudgetWorkload workload;

    /**
     * 创建只允许 {@link LobStorage} 发布的有效计划，避免半初始化计划跨越 redo admission 边界。
     *
     * @param segment  authoritative LOB segment；不能为 {@code null}。
     * @param targets  已完成基本 identity 校验和稳定排序的非空 ownership 列表。
     * @param workload 由全部 reference page count 推导的正 redo workload。
     * @throws DatabaseValidationException 任一字段缺失、target 列表为空或包含 {@code null} 时抛出。
     */
    LobFreeBatchPlan(SegmentRef segment, List<LobFreeTarget> targets, RedoBudgetWorkload workload) {
        if (segment == null || targets == null || targets.isEmpty()
                || targets.stream().anyMatch(java.util.Objects::isNull) || workload == null) {
            throw new DatabaseValidationException("LOB free batch plan fields are invalid");
        }
        this.segment = segment;
        this.targets = List.copyOf(targets);
        this.workload = workload;
    }

    /**
     * 返回释放授权使用的 authoritative segment。
     *
     * @return exact DD binding 中与全部 target 属于同一 tablespace/segment 的稳定 identity。
     */
    public SegmentRef segment() { return segment; }

    /**
     * 返回批量释放目标。列表和元素均为不可变值对象，调用方不能在 admission 后增删 ownership。
     *
     * @return 按 external first-page/ordinal 排序的非空 ownership 列表；绝不返回 {@code null}。
     */
    public List<LobFreeTarget> targets() { return targets; }

    /**
     * 返回本批次的 redo admission 上界。
     *
     * @return 覆盖一次 segment/FSP 固定成本和全部 page free/PAGE_INIT 的动态 workload。
     */
    public RedoBudgetWorkload workload() { return workload; }
}
