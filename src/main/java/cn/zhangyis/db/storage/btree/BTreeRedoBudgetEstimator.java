package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;

/**
 * B+Tree 物理写 workload 估算器。它只读取 begin 前已有的 rootLevel，不访问页面、不申请空间、不触发 capacity 等待。
 * 公式按树高覆盖 split propagation、parent rewrite、merge/redistribute 与 root grow/shrink 的最坏触页数量。
 */
public final class BTreeRedoBudgetEstimator {

    private BTreeRedoBudgetEstimator() {
    }

    /** insert/split：4 个固定叶/root 余量，加每层 6 个 page-image 等价量。 */
    public static RedoBudgetWorkload insert(int rootLevel) {
        return scaled(rootLevel, 4, 6);
    }

    /** physical delete/merge：6 个固定 sibling/FSP 余量，加每层 6 个结构传播等价量。 */
    public static RedoBudgetWorkload structuralDelete(int rootLevel) {
        return scaled(rootLevel, 6, 6);
    }

    /** 等长 replace/delete-mark 等不触发 SMO 的单叶页改写。 */
    public static RedoBudgetWorkload pointRewrite() {
        return RedoBudgetWorkload.pageImages(4);
    }

    private static RedoBudgetWorkload scaled(int rootLevel, long base, long perLevel) {
        if (rootLevel < 0) {
            throw new DatabaseValidationException("B+Tree redo budget root level must not be negative: "
                    + rootLevel);
        }
        try {
            long height = Math.addExact((long) rootLevel, 1L);
            return RedoBudgetWorkload.pageImages(Math.addExact(base, Math.multiplyExact(perLevel, height)));
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("B+Tree redo workload overflows at root level "
                    + rootLevel, error);
        }
    }
}
