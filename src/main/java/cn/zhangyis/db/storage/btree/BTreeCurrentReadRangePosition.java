package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.trx.lock.GapLockKey;

import java.util.List;
import java.util.Optional;

/**
 * B+Tree range current-read 的短 MTR 定位快照。它只保存已物化记录的锁落点和范围终止 gap，
 * 不持有页游标、page latch 或 buffer fix，因此可安全跨越事务锁等待。
 *
 * @param records     按索引顺序排列的当前版本记录落点；每项都带 record/next-key key。
 * @param terminalGap RR 范围锁定读用于防幻读的终止 gap；RC 调用方会忽略该字段。
 */
record BTreeCurrentReadRangePosition(List<BTreeCurrentReadPosition> records,
                                     Optional<GapLockKey> terminalGap) {

    BTreeCurrentReadRangePosition {
        if (records == null || terminalGap == null) {
            throw new DatabaseValidationException("current-read range fields must not be null");
        }
        records = List.copyOf(records);
        for (BTreeCurrentReadPosition position : records) {
            if (position == null || !position.hit()) {
                throw new DatabaseValidationException("current-read range records must be hit positions");
            }
        }
    }
}
