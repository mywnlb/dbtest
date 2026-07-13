package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.trx.UndoRedoBudgetEstimator;
import cn.zhangyis.db.storage.trx.UndoWritePlan;

/** 单行 clustered DML 的 redo workload 组合器：B+Tree 结构部分 + 同批 undo append 部分。 */
final class DmlRedoBudgetEstimator {

    private DmlRedoBudgetEstimator() {
    }

    static RedoBudgetWorkload insert(BTreeIndex index, UndoWritePlan undoPlan) {
        requireIndex(index);
        requirePlan(undoPlan);
        return BTreeRedoBudgetEstimator.insert(index.rootLevel())
                .plus(undoPlan.redoWorkload());
    }

    /** 兼容既有 estimator 单元测试；生产 DML 必须使用携带 external 页数的计划重载。 */
    static RedoBudgetWorkload insert(BTreeIndex index, boolean firstUndoWrite) {
        requireIndex(index);
        return BTreeRedoBudgetEstimator.insert(index.rootLevel())
                .plus(UndoRedoBudgetEstimator.append(firstUndoWrite));
    }

    static RedoBudgetWorkload pointRewrite(BTreeIndex index, UndoWritePlan undoPlan) {
        requireIndex(index);
        requirePlan(undoPlan);
        return BTreeRedoBudgetEstimator.pointRewrite()
                .plus(undoPlan.redoWorkload());
    }

    /** 兼容既有 estimator 单元测试；不用于 external-aware 生产 admission。 */
    static RedoBudgetWorkload pointRewrite(BTreeIndex index, boolean firstUndoWrite) {
        requireIndex(index);
        return BTreeRedoBudgetEstimator.pointRewrite()
                .plus(UndoRedoBudgetEstimator.append(firstUndoWrite));
    }

    private static void requireIndex(BTreeIndex index) {
        if (index == null) {
            throw new DatabaseValidationException("DML redo budget index must not be null");
        }
    }

    private static void requirePlan(UndoWritePlan plan) {
        if (plan == null) {
            throw new DatabaseValidationException("DML undo write plan must not be null");
        }
    }
}
