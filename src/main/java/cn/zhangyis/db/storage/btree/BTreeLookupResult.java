package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.RecordRef;

/**
 * B+Tree lookup/scan 的物化结果。结果中只保存记录字节物化后的逻辑值和短期定位，
 * 不保存 page latch、buffer fix、RecordPage 或 RecordCursor，避免调用方跨 MTR 生命周期使用内部资源。
 *
 * @param index     产生该结果的索引描述。
 * @param recordRef 页内短期定位；reorganize/split 后可能失效，长期使用必须按 key 重新定位。
 * @param record    已从页内记录拷贝并解码出来的逻辑记录。
 */
public record BTreeLookupResult(BTreeIndex index, RecordRef recordRef, LogicalRecord record) {

    public BTreeLookupResult {
        if (index == null || recordRef == null || record == null) {
            throw new DatabaseValidationException("btree lookup result index/ref/record must not be null");
        }
    }
}
