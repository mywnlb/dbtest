package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.trx.UndoRedoBudgetEstimator;

/** 单行 clustered DML 的 redo workload 组合器：B+Tree 结构部分 + 同批 undo append 部分。 */
final class DmlRedoBudgetEstimator {

    private DmlRedoBudgetEstimator() {
    }

    static RedoBudgetWorkload insert(BTreeIndex index, boolean firstUndoWrite) {
        requireIndex(index);
        return BTreeRedoBudgetEstimator.insert(index.rootLevel())
                .plus(UndoRedoBudgetEstimator.append(firstUndoWrite));
    }

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
}
