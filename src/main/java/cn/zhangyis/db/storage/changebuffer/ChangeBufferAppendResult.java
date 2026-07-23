package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;

/**
 * 全局树 append 的稳定结果；mutation 携带本次分配的 sequence，indexAfter 携带可能发生 root split 的新 level。
 *
 * @param mutation 已写入全局树的不可变 mutation
 * @param indexAfter 插入完成后的内部树 descriptor
 */
public record ChangeBufferAppendResult(ChangeBufferMutation mutation, BTreeIndex indexAfter) {
    public ChangeBufferAppendResult {
        if (mutation == null || indexAfter == null) {
            throw new DatabaseValidationException("change buffer append result fields must not be null");
        }
    }
}
