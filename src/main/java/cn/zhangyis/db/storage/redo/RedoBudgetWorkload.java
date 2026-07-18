package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 领域 estimator 交给 MTR admission 的纯编码工作量，不包含 page size、capacity 或存储模块类型。
 *
 * @param pageImageEquivalents 以“一条完整 PAGE_BYTES after-image”计量的保守工作量。
 * @param extraLogicalBytes 不能折算为页 image 的额外逻辑 redo 字节上界。
 */
public record RedoBudgetWorkload(long pageImageEquivalents, long extraLogicalBytes) {

    public RedoBudgetWorkload {
        if (pageImageEquivalents < 0 || extraLogicalBytes < 0) {
            throw new DatabaseValidationException("redo workload values must not be negative: pages="
                    + pageImageEquivalents + ", extra=" + extraLogicalBytes);
        }
    }

    /** 创建只含完整页等价量的 workload。
     *
     * @param pageImageEquivalents redo 预算计算使用的非负工作量上界 {@code pageImageEquivalents}；必须保守覆盖实际写入量，且累加时不得溢出
     * @return {@code pageImages} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public static RedoBudgetWorkload pageImages(long pageImageEquivalents) {
        return new RedoBudgetWorkload(pageImageEquivalents, 0);
    }

    /** checked 合并两个独立领域子计划。
     *
     * @param other redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code plus} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RedoBudgetWorkload plus(RedoBudgetWorkload other) {
        if (other == null) {
            throw new DatabaseValidationException("redo workload to combine must not be null");
        }
        try {
            return new RedoBudgetWorkload(
                    Math.addExact(pageImageEquivalents, other.pageImageEquivalents),
                    Math.addExact(extraLogicalBytes, other.extraLogicalBytes));
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("redo workload combination overflows", error);
        }
    }
}
