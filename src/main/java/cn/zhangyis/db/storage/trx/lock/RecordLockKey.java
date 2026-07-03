package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.record.page.RecordRef;

/**
 * 已存在记录的事务锁定位。当前切片使用物理 {@code indexId + pageId + heapNo} 表达锁点；
 * 后续 B+Tree current-read 等待返回后必须重新定位并校验记录，不能长期相信旧 page offset。
 *
 * @param indexId 记录所属索引 id，来自 {@link RecordRef#indexId()} 或索引 metadata。
 * @param pageId  记录所在数据页，限定 heapNo 的页内作用域。
 * @param heapNo  页内 heap 序号，结构变更后必须由调用方重新校验。
 */
public record RecordLockKey(long indexId, PageId pageId, int heapNo) implements TransactionLockKey {

    public RecordLockKey {
        if (indexId < 0) {
            throw new DatabaseValidationException("record lock indexId must be non-negative: " + indexId);
        }
        if (pageId == null) {
            throw new DatabaseValidationException("record lock pageId must not be null");
        }
        if (heapNo < 0) {
            throw new DatabaseValidationException("record lock heapNo must be non-negative: " + heapNo);
        }
    }

    /**
     * 从页内记录引用构造 record lock key。这里只保留锁兼容所需的 index/page/heapNo，
     * 不使用可能因 page reorganize 失效的 pageOffset。
     *
     * @param ref record 模块返回的记录定位。
     * @return record 事务锁 key。
     */
    public static RecordLockKey from(RecordRef ref) {
        if (ref == null) {
            throw new DatabaseValidationException("record ref must not be null");
        }
        return new RecordLockKey(ref.indexId(), ref.pageId(), ref.heapNo());
    }
}
