package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.record.page.RecordRef;

import java.util.List;

/**
 * B+Tree 插入结果。只返回页内短期定位值，不返回页句柄或 cursor；
 * 后续 split/reorganize 后调用方必须按 key 重新定位。
 *
 * @param index            调用时传入的索引描述。
 * @param recordRef        新插入记录的页内短期定位；split 后也只可短期诊断，长期使用仍需按 key 重新定位。
 * @param indexAfterInsert 插入后调用方应继续使用的索引元数据快照；root split 时 rootLevel 会从 0 提升到 1。
 * @param splitOccurred    true 表示本次插入触发了页 split，调用方和测试可据此观察结构变化。
 * @param allocatedPages   本次 split 分配的新页；用于诊断和测试，不表达长期所有权。
 */
public record BTreeInsertResult(BTreeIndex index, RecordRef recordRef,
                                BTreeIndex indexAfterInsert, boolean splitOccurred,
                                List<PageId> allocatedPages) {

    /**
     * B1/B2 兼容构造器。未发生 split 时插入前后的索引元数据相同，也不携带新分配页。
     */
    public BTreeInsertResult(BTreeIndex index, RecordRef recordRef) {
        this(index, recordRef, index, false, List.of());
    }

    public BTreeInsertResult {
        if (index == null || recordRef == null || indexAfterInsert == null || allocatedPages == null) {
            throw new DatabaseValidationException("btree insert result fields must not be null");
        }
        allocatedPages = List.copyOf(allocatedPages);
    }
}
