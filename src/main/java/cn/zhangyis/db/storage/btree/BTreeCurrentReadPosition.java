package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.trx.lock.GapLockKey;
import cn.zhangyis.db.storage.trx.lock.NextKeyLockKey;
import cn.zhangyis.db.storage.trx.lock.RecordLockKey;

import java.util.Optional;

/**
 * B+Tree 短 MTR 定位 current-read 落点后的不可变快照。该对象只包含物化记录和事务锁 key，
 * 不包含 RecordCursor、RecordPage、PageGuard 或 IndexPageHandle，因此可在释放 page latch/fix 后继续用于 LockManager。
 *
 * @param record  命中记录；miss 时为空。
 * @param recordKey 命中记录的 record lock key；miss 时为空。
 * @param gapKey  命中记录前序 gap 或 miss 目标 gap。
 * @param nextKey 命中时的 next-key key；本片主要供后续 range current-read 使用。
 */
record BTreeCurrentReadPosition(Optional<BTreeLookupResult> record, Optional<RecordLockKey> recordKey,
                                GapLockKey gapKey, Optional<NextKeyLockKey> nextKey) {

    BTreeCurrentReadPosition {
        if (record == null || recordKey == null || gapKey == null || nextKey == null) {
            throw new DatabaseValidationException("current-read position fields must not be null");
        }
        if (record.isPresent() != recordKey.isPresent()) {
            throw new DatabaseValidationException("current-read record and recordKey presence must match");
        }
        if (record.isPresent() != nextKey.isPresent()) {
            throw new DatabaseValidationException("current-read record and nextKey presence must match");
        }
    }

    boolean hit() {
        return record.isPresent();
    }
}
