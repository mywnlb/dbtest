package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * next-key 锁定位，语义上是“某条已存在记录 + 该记录前序 gap”。兼容判断会同时检查 record 与 gap 部分，
 * 因而可阻塞对该记录的 X/S 冲突请求以及对前序 gap 的插入意向。
 *
 * @param recordKey 被保护的已存在记录。
 * @param gapKey    被保护的前序 gap。
 */
public record NextKeyLockKey(RecordLockKey recordKey, GapLockKey gapKey) implements TransactionLockKey {

    public NextKeyLockKey {
        if (recordKey == null) {
            throw new DatabaseValidationException("next-key recordKey must not be null");
        }
        if (gapKey == null) {
            throw new DatabaseValidationException("next-key gapKey must not be null");
        }
        if (recordKey.indexId() != gapKey.indexId()) {
            throw new DatabaseValidationException("next-key record and gap must belong to same index");
        }
    }

    @Override
    public long indexId() {
        return recordKey.indexId();
    }
}
