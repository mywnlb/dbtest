package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.SegmentRef;

/** catalog 持久化的索引物理定位：稳定 root page 加 leaf/non-leaf segment 身份。 */
public record IndexStorageBinding(long indexId, PageId rootPageId, int rootLevel,
                                  SegmentRef leafSegment, SegmentRef nonLeafSegment) {
    public IndexStorageBinding {
        if (indexId <= 0 || rootPageId == null || rootLevel < 0 || leafSegment == null || nonLeafSegment == null) {
            throw new DatabaseValidationException("invalid index storage binding");
        }
    }
}
