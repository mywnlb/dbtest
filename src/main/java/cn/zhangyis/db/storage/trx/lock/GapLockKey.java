package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.page.SearchKey;

/**
 * 索引 key 区间上的 gap 锁定位。{@code leftKey == null} 表示负无穷边界，{@code rightKey == null}
 * 表示正无穷边界；这是当前教学实现的简化，后续可替换为显式 infimum/supremum 边界值对象。
 *
 * @param indexId  gap 所属索引，决定分片与锁冲突作用域。
 * @param leftKey  gap 左边界；null 表示索引负无穷。
 * @param rightKey gap 右边界；null 表示索引正无穷。
 */
public record GapLockKey(long indexId, SearchKey leftKey, SearchKey rightKey) implements TransactionLockKey {

    public GapLockKey {
        if (indexId < 0) {
            throw new DatabaseValidationException("gap lock indexId must be non-negative: " + indexId);
        }
    }
}
