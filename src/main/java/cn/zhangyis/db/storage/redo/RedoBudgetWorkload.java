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

    /** 创建只含完整页等价量的 workload。 */
    public static RedoBudgetWorkload pageImages(long pageImageEquivalents) {
        return new RedoBudgetWorkload(pageImageEquivalents, 0);
    }

    /** checked 合并两个独立领域子计划。 */
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
