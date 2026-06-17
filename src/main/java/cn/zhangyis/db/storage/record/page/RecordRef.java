package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

/**
 * 页内记录的稳定定位值（innodb-record-design §5.1，不可变）。组合页号、heap 序号、页内偏移与版本/索引信息。
 *
 * <p>注意：{@code pageOffset} 是物理定位，page reorganize / split / purge 后可能失效；
 * heapNo 是页内物理序号、不代表 key 顺序。长期持有应优先保存 key 或主键，旧 RecordRef 在结构变更后必须重新校验。
 *
 * @param pageId        所在页。
 * @param heapNo        页内 heap 物理序号（≥0）。
 * @param pageOffset    页内字节偏移（≥0，指向 RecordHeader 起始）。
 * @param schemaVersion 解码该记录所需的 schema 版本（≥0）。
 * @param indexId       所属索引 id（≥0）。
 */
public record RecordRef(PageId pageId, int heapNo, int pageOffset, long schemaVersion, long indexId) {

    public RecordRef {
        if (pageId == null) {
            throw new DatabaseValidationException("record ref pageId must not be null");
        }
        if (heapNo < 0) {
            throw new DatabaseValidationException("record ref heapNo must be non-negative: " + heapNo);
        }
        if (pageOffset < 0) {
            throw new DatabaseValidationException("record ref pageOffset must be non-negative: " + pageOffset);
        }
        if (schemaVersion < 0) {
            throw new DatabaseValidationException("record ref schemaVersion must be non-negative: " + schemaVersion);
        }
        if (indexId < 0) {
            throw new DatabaseValidationException("record ref indexId must be non-negative: " + indexId);
        }
    }
}
