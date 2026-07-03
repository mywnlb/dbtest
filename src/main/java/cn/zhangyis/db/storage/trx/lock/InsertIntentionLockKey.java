package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 插入意向锁定位。它不代表最终插入的 record heapNo，而是先声明要进入某个 gap；
 * 同 gap 的多个插入意向可共存，真正插入时仍需 B+Tree/record 层重新定位并处理唯一性。
 *
 * @param gapKey 插入目标 gap。
 */
public record InsertIntentionLockKey(GapLockKey gapKey) implements TransactionLockKey {

    public InsertIntentionLockKey {
        if (gapKey == null) {
            throw new DatabaseValidationException("insert intention gapKey must not be null");
        }
    }

    @Override
    public long indexId() {
        return gapKey.indexId();
    }
}
